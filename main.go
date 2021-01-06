// Data aggregation for rtlamr.
// Copyright (C) 2017 Douglas Hall
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
	"go.etcd.io/bbolt"
	"golang.org/x/xerrors"
)

const threshold = 30 * time.Second

// LogMessage is an encapsulating type rtlamr uses for all messages. It contains
// time, message type, and the encapsulated message.
type LogMessage struct {
	Time time.Time
	Type string

	// Defer decoding until the message type is known.
	Message json.RawMessage
}

func (msg LogMessage) String() string {
	return fmt.Sprintf("{Time:%s Type:%s}", msg.Time, msg.Type)
}

// IDM handles Interval Data Messages (IDM and NetIDM) from rtlamr.
type IDM struct {
	Meters MeterMap `json:"-"`

	EndpointType byte     `json:"ERTType"`
	EndpointID   uint32   `json:"ERTSerialNumber"`
	TransmitTime uint16   `json:"TransmitTimeOffset"`
	IntervalIdx  byte     `json:"ConsumptionIntervalCount"`
	IntervalDiff []uint16 `json:"DifferentialConsumptionIntervals"`
	Outage       []byte   `json:"PowerOutageFlags"`

	IDMConsumption       uint32 `json:"LastConsumptionCount"`
	NetIDMConsumption    uint32 `json:"LastConsumption"`
	NetIDMConsumptionNet uint32 `json:"LastConsumptionNet"`
	NetIDMGeneration     uint32 `json:"LastGeneration"`
}

// AddPoints adds differential usage data to a batch of points.
func (idm IDM) AddPoints(msg LogMessage, eachFn EachFn) {
	// TransmitTime is 1/16ths of a second since the interval began.
	intervalOffset := time.Duration(idm.TransmitTime) * time.Second / 16

	meter := Meter{idm.EndpointID, idm.EndpointType, msg.Type}

	// Does this meter have any state?
	state, seen := idm.Meters.m[meter]

	// Update the meter map with new state.
	idm.Meters.Update(
		meter,
		LastMessage{
			msg.Time.Add(-intervalOffset),
			uint(idm.IntervalIdx),
		},
	)

	// Convert outage flags (6 bytes) to uint64 (8 bytes)
	outageBytes := make([]uint8, 8)
	copy(outageBytes[2:], idm.Outage)
	outage := binary.BigEndian.Uint64(outageBytes)

	tags := map[string]string{
		"protocol":      msg.Type,
		"msg_type":      "cumulative",
		"endpoint_type": strconv.Itoa(int(idm.EndpointType)),
		"endpoint_id":   strconv.Itoa(int(idm.EndpointID)),
	}

	fields := map[string]interface{}{
		"consumption": int64(idm.IDMConsumption),
	}

	if msg.Type == "NetIDM" {
		fields["consumption"] = int64(idm.NetIDMConsumption)
		fields["generation"] = int64(idm.NetIDMGeneration)
		fields["consumption_net"] = int64(idm.NetIDMConsumptionNet)
	}

	eachFn(msg.Time.Add(-intervalOffset), tags, fields)

	// Re-use tags from cumulative message.
	tags["msg_type"] = "differential"

	// For each differential interval.
	for idx, usage := range idm.IntervalDiff {
		// Calculate the interval.
		interval := uint(int(idm.IntervalIdx)-idx) % 256

		// Calculate the interval's timestamp.
		intervalTime := msg.Time.Add(-time.Duration(idx)*5*time.Minute - intervalOffset)

		// If the meter has been seen before and we are looking at the same interval.
		if seen && interval == state.Interval {
			// Calculate the time difference between the current interval, and
			// the last interval we know about.
			diff := state.Time.Sub(intervalTime)

			// If the difference is less than the threshold, this interval is old data, bail.
			if diff > -threshold && diff < threshold {
				return
			}
		}

		fields := map[string]interface{}{
			"consumption": int64(usage),
			"interval":    int64(interval),
		}

		// If the outage bit corresponding to this interval is 1, add it to the field.
		if (outage>>uint(46-idx))&1 == 1 {
			fields["outage"] = int64(1)
		}

		eachFn(intervalTime, tags, fields)
	}
}

// SCM handles Standard Consumption Messages from rtlamr.
type SCM struct {
	EndpointID   uint32 `json:"ID"`
	EndpointType uint8  `json:"Type"`
	Consumption  uint32 `json:"Consumption"`
}

// AddPoints adds cumulative usage data to a batch of points.
func (scm SCM) AddPoints(msg LogMessage, eachFn EachFn) {
	tags := map[string]string{
		"protocol":      msg.Type,
		"msg_type":      "cumulative",
		"endpoint_type": strconv.Itoa(int(scm.EndpointType)),
		"endpoint_id":   strconv.Itoa(int(scm.EndpointID)),
	}
	fields := map[string]interface{}{
		"consumption": int64(scm.Consumption),
	}
	eachFn(msg.Time, tags, fields)
}

// SCMPlus handles Standard Consumption Message Plus messages from rtlamr.
type SCMPlus struct {
	EndpointID   uint32 `json:"EndpointID"`
	EndpointType uint8  `json:"EndpointType"`
	Consumption  uint32 `json:"Consumption"`
}

// AddPoints adds cumulative usage data to a batch of points.
func (scmplus SCMPlus) AddPoints(msg LogMessage, eachFn EachFn) {
	tags := map[string]string{
		"protocol":      msg.Type,
		"msg_type":      "cumulative",
		"endpoint_type": strconv.Itoa(int(scmplus.EndpointType)),
		"endpoint_id":   strconv.Itoa(int(scmplus.EndpointID)),
	}
	fields := map[string]interface{}{
		"consumption": int64(scmplus.Consumption),
	}

	eachFn(msg.Time, tags, fields)
}

// R900 handles Neptune R900 messages from rtlamr, both R900 and R900BCD.
type R900 struct {
	EndpointID   uint32 `json:"ID"`
	EndpointType uint8  `json:"Unkn1"`
	Consumption  uint32 `json:"Consumption"`

	NoUse    uint8 `json:"NoUse"`    // Day bins of no use
	BackFlow uint8 `json:"BackFlow"` // Backflow past 35d hi/lo
	Leak     uint8 `json:"Leak"`     // Day bins of leak
	LeakNow  uint8 `json:"LeakNow"`  // Leak past 24h hi/lo
}

// AddPoints adds cummulative usage data to a batch of points.
func (r900 R900) AddPoints(msg LogMessage, eachFn EachFn) {
	tags := map[string]string{
		"protocol":      msg.Type,
		"msg_type":      "cumulative",
		"endpoint_type": strconv.Itoa(int(r900.EndpointType)),
		"endpoint_id":   strconv.Itoa(int(r900.EndpointID)),
	}

	fields := map[string]interface{}{
		"consumption": int64(r900.Consumption),
		"nouse":       int64(r900.NoUse),
		"backflow":    int64(r900.BackFlow),
		"leak":        int64(r900.Leak),
		"leak_now":    int64(r900.LeakNow),
	}

	eachFn(msg.Time, tags, fields)
}

// Message knows how to add points to a batch of points.
type Message interface {
	AddPoints(LogMessage, EachFn)
}

type EachFn func(t time.Time, tags map[string]string, fields map[string]interface{})

type Meter struct {
	EndpointID   uint32
	EndpointType uint8
	Protocol     string
}

// LastMessage represents a meter's last interval and time.
type LastMessage struct {
	Time     time.Time
	Interval uint
}

// MeterMap keeps meter state to avoid sending duplicate data to the database.
type MeterMap struct {
	db *bbolt.DB
	m  map[Meter]LastMessage
}

func NewMeterMap(filename string) (m MeterMap, err error) {
	m = MeterMap{
		m: map[Meter]LastMessage{},
	}

	m.db, err = bbolt.Open(filename, 0600, nil)
	if err != nil {
		return m, xerrors.Errorf("bbolt.Open: %w", err)
	}

	err = m.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte("meters"))
		if bkt == nil {
			return nil
		}

		err = bkt.ForEach(func(k, v []byte) error {
			var (
				meter Meter
				msg   LastMessage
			)

			err := msgpack.Unmarshal(k, &meter)
			if err != nil {
				return xerrors.Errorf("msgpack.Unmarshal: %w", err)
			}

			err = msgpack.Unmarshal(v, &msg)
			if err != nil {
				return xerrors.Errorf("msgpack.Unmarshal: %w", err)
			}

			m.m[meter] = msg

			return nil
		})

		return nil
	})
	if err != nil {
		return m, xerrors.Errorf("m.db.View: %w", err)
	}

	return m, nil
}

func (m *MeterMap) Update(meter Meter, msg LastMessage) (err error) {
	err = m.db.Update(func(tx *bbolt.Tx) error {
		tx.OnCommit(func() {
			m.m[meter] = msg
		})

		bkt, err := tx.CreateBucketIfNotExists([]byte("meters"))
		if err != nil {
			return xerrors.Errorf("tx.CreateBucketIfNotExists: %w", err)
		}

		key, err := msgpack.Marshal(meter)
		if err != nil {
			return xerrors.Errorf("msgpack.Marshal: %w", err)
		}

		val, err := msgpack.Marshal(msg)
		if err != nil {
			return xerrors.Errorf("msgpack.Marshal: %w", err)
		}

		err = bkt.Put(key, val)
		if err != nil {
			return xerrors.Errorf("bkt.Put: %w", err)
		}

		return nil
	})
	if err != nil {
		return xerrors.Errorf("m.db.View: %w", err)
	}

	return nil
}

func lookupEnv(name string, dryRun bool) string {
	val, ok := os.LookupEnv(name)
	if !ok && !dryRun {
		log.Fatalf("%q undefined\n", name)
	}
	return val
}

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	// COLLECT_INFLUXDB_STRICTIDM limits which endpoint types may be decoded
	// between IDM and NetIDM. In the wild, type 7 should be standard IDM and
	// type 8 should be NetIDM. Both messages have the same preamble and
	// checksum, so they are picked up by both decoders, but have different
	// internal field layout.
	_, strict := os.LookupEnv("COLLECT_STRICTIDM")
	_, dryRun := os.LookupEnv("COLLECT_INFLUXDB_DRYRUN")

	hostname := lookupEnv("COLLECT_INFLUXDB_HOSTNAME", dryRun)
	token := lookupEnv("COLLECT_INFLUXDB_TOKEN", dryRun)
	org := lookupEnv("COLLECT_INFLUXDB_ORG", dryRun)
	bucket := lookupEnv("COLLECT_INFLUXDB_BUCKET", dryRun)
	measurement := lookupEnv("COLLECT_INFLUXDB_MEASUREMENT", dryRun)

	opts := influxdb2.DefaultOptions()

	clientCertFile, ok := os.LookupEnv("COLLECT_INFLUXDB_CLIENT_CERT")
	if ok && !dryRun {
		clientKeyFile := lookupEnv("COLLECT_INFLUXDB_CLIENT_KEY", dryRun)
		clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			log.Fatalf("could not load client certificate: %s\n", err)
		}

		opts.SetTLSConfig(&tls.Config{
			Certificates: []tls.Certificate{clientCert},
		})
	}

	mm, err := NewMeterMap("meters.db")
	if err != nil {
		log.Fatalf("%+v\n", xerrors.Errorf("NewMeterMap: %w", err))
	}
	defer mm.db.Close()

	var client influxdb2.Client

	if !dryRun {
		log.Printf("connecting to %q", hostname)
		client = influxdb2.NewClientWithOptions(hostname, token, opts)
		defer client.Close()
	}

	// Create a blocking write api.
	api := client.WriteAPIBlocking(org, bucket)

	// Read lines from stdin.
	stdinBuf := bufio.NewScanner(os.Stdin)
	for stdinBuf.Scan() {
		line := stdinBuf.Bytes()

		// Parse a log message.
		var logMsg LogMessage
		err := json.Unmarshal(line, &logMsg)
		if err != nil {
			log.Println(err)
			continue
		}

		// Store the appropriate message type in msg based on logMsg.Type.
		var msg Message
		switch logMsg.Type {
		case "SCM":
			msg = new(SCM)
		case "SCM+":
			msg = new(SCMPlus)
		case "IDM", "NetIDM":
			msg = new(IDM)
		case "R900", "R900BCD":
			msg = new(R900)
		}

		// Parse the encapsulated message.
		err = json.Unmarshal(logMsg.Message, msg)
		if err != nil {
			log.Println(errors.Wrap(err, "json unmarshal"))
			continue
		}

		// If current message is an IDM.
		if idm, ok := msg.(*IDM); ok {
			// Store meter state for discarding duplicate data.
			idm.Meters = mm

			// If COLLECT_INFLUXDB_STRICTIDM is defined, disallow IDM of type 8.
			if strict && logMsg.Type == "IDM" && idm.EndpointType == 8 {
				continue
			}

			// If COLLECT_INFLUXDB_STRICTIDM is defined, disallow NetIDM of type 7.
			if strict && logMsg.Type == "NetIDM" && idm.EndpointType == 7 {
				continue
			}
		}

		pts := []*write.Point{}

		// Messages know how to add points to a batch.
		msg.AddPoints(logMsg, func(t time.Time, tags map[string]string, fields map[string]interface{}) {
			pt := write.NewPoint(measurement, tags, fields, t)
			pts = append(pts, pt)
		})

		if !dryRun {
			err = api.WritePoint(context.Background(), pts...)
			if err != nil {
				log.Fatalf("%+v\n", xerrors.Errorf("api.WritePoint: %w", err))
			}
		}
	}
}
