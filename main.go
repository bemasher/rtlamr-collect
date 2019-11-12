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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/pkg/errors"
)

const (
	measurement = "rtlamr"
	threshold   = 30 * time.Second
)

// LogMessage is a encapsulating type rtlamr uses for all messages. It contains
// time, message type, and the encapsulated message.
type LogMessage struct {
	Time time.Time
	Type string

	// Defer decoding until we know what type the message is.
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

	Consumption    uint32 `json:"LastConsumption"`
	ConsumptionNet uint32 `json:"LastConsumptionNet"`
	Generation     uint32 `json:"LastGeneration"`
}

// AddPoints adds differential usage data to a batch of points.
func (idm IDM) AddPoints(msg LogMessage, bp client.BatchPoints) {
	// TransmitTime is 1/16ths of a second since the interval began.
	intervalOffset := time.Duration(idm.TransmitTime) * time.Second / 16

	// Does this meter have any state?
	state, seen := idm.Meters[Meter{idm.EndpointID, idm.EndpointType, msg.Type}]

	// Update the meter map with new state.
	idm.Meters[Meter{idm.EndpointID, idm.EndpointType, msg.Type}] = LastMessage{
		msg.Time.Add(-intervalOffset),
		uint(idm.IntervalIdx),
	}

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
		"consumption": int64(idm.Consumption),
	}

	if msg.Type == "NetIDM" {
		fields["generation"] = int64(idm.Generation)
		fields["consumption_net"] = int64(idm.ConsumptionNet)
	}

	pt, err := client.NewPoint(measurement, tags, fields, msg.Time.Add(-intervalOffset))
	if err != nil {
		// I'm not sure what kinds of errors we might encounter when making a
		// new point, but don't hard-stop if we encounter one. Most other
		// operations should still succeed.
		log.Println(errors.Wrap(err, "new point"))
	} else {
		bp.AddPoint(pt)
	}

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

		pt, err := client.NewPoint(
			measurement,
			tags,
			fields,
			intervalTime,
		)

		if err != nil {
			log.Println(errors.Wrap(err, "new point"))
			continue
		}

		bp.AddPoint(pt)
	}
}

// SCM handles Standard Consumption Messages from rtlamr.
type SCM struct {
	EndpointID   uint32 `json:"ID"`
	EndpointType uint8  `json:"Type"`
	Consumption  uint32 `json:"Consumption"`
}

// AddPoints adds cummulative usage data to a batch of points.
func (scm SCM) AddPoints(msg LogMessage, bp client.BatchPoints) {
	pt, err := client.NewPoint(
		measurement,
		map[string]string{
			"protocol":      msg.Type,
			"msg_type":      "cumulative",
			"endpoint_type": strconv.Itoa(int(scm.EndpointType)),
			"endpoint_id":   strconv.Itoa(int(scm.EndpointID)),
		},
		map[string]interface{}{
			"consumption": int64(scm.Consumption),
		},
		msg.Time,
	)

	if err != nil {
		log.Println(errors.Wrap(err, "new point"))
		return
	}

	bp.AddPoint(pt)
}

// SCMPlus handles Standard Consumption Message Plus messages from rtlamr.
type SCMPlus struct {
	EndpointID   uint32 `json:"EndpointID"`
	EndpointType uint8  `json:"EndpointType"`
	Consumption  uint32 `json:"Consumption"`
}

// AddPoints adds cummulative usage data to a batch of points.
func (scmplus SCMPlus) AddPoints(msg LogMessage, bp client.BatchPoints) {
	pt, err := client.NewPoint(
		measurement,
		map[string]string{
			"protocol":      msg.Type,
			"msg_type":      "cumulative",
			"endpoint_type": strconv.Itoa(int(scmplus.EndpointType)),
			"endpoint_id":   strconv.Itoa(int(scmplus.EndpointID)),
		},
		map[string]interface{}{
			"consumption": int64(scmplus.Consumption),
		},
		msg.Time,
	)

	if err != nil {
		log.Println(errors.Wrap(err, "new point"))
		return
	}

	bp.AddPoint(pt)
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
func (r900 R900) AddPoints(msg LogMessage, bp client.BatchPoints) {
	pt, err := client.NewPoint(
		measurement,
		map[string]string{
			"protocol":      msg.Type,
			"msg_type":      "cumulative",
			"endpoint_type": strconv.Itoa(int(r900.EndpointType)),
			"endpoint_id":   strconv.Itoa(int(r900.EndpointID)),
		},
		map[string]interface{}{
			"consumption": int64(r900.Consumption),
			"nouse":       int64(r900.NoUse),
			"backflow":    int64(r900.BackFlow),
			"leak":        int64(r900.Leak),
			"leak_now":    int64(r900.LeakNow),
		},
		msg.Time,
	)

	if err != nil {
		log.Println()
		return
	}

	bp.AddPoint(pt)
}

// Message knows how to add points to a batch of points.
type Message interface {
	AddPoints(LogMessage, client.BatchPoints)
}

type Meter struct {
	EndpointID   uint32
	EndpointType uint8
	Protocol     string
}

func NewMeter(tags map[string]string) (m Meter, err error) {
	endpoint_id, err := strconv.ParseUint(tags["endpoint_id"], 10, 32)
	if err != nil {
		return m, errors.Wrap(err, "new meter")
	}

	endpoint_type, err := strconv.ParseUint(tags["endpoint_type"], 10, 8)
	if err != nil {
		return m, errors.Wrap(err, "new meter")
	}

	protocol, ok := tags["protocol"]
	if !ok {
		return m, errors.New("missing protocol tag")
	}

	return Meter{
		uint32(endpoint_id),
		uint8(endpoint_type),
		protocol,
	}, nil
}

// LastMessage represents a meter's last interval and time.
type LastMessage struct {
	Time     time.Time
	Interval uint
}

// MeterMap keeps meter state to avoid sending duplicate data to the database.
type MeterMap map[Meter]LastMessage

// Preload retrieves state for all previously seen differential meters.
func (mm MeterMap) Preload(c client.Client, database string) {
	q := client.NewQuery(`
		SELECT last(interval)
		FROM rtlamr
		WHERE msg_type = 'differential'
		GROUP BY protocol, endpoint_id, endpoint_type
	`, database, "s")

	res, err := c.Query(q)
	if err != nil {
		log.Println(errors.Wrap(err, "meter map preload"))
		return
	}

	for _, r := range res.Results {
		for _, s := range r.Series {
			meter, err := NewMeter(s.Tags)
			if err != nil {
				log.Println(errors.Wrap(err, "new meter"))
				continue
			}

			for _, v := range s.Values {
				unixEpoch, _ := v[0].(json.Number).Int64()
				interval, _ := v[1].(json.Number).Int64()

				mm[meter] = LastMessage{
					time.Unix(unixEpoch, 0),
					uint(interval),
				}
			}
		}
	}
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

	username := lookupEnv("COLLECT_INFLUXDB_USER", dryRun)
	password := lookupEnv("COLLECT_INFLUXDB_PASS", dryRun)
	hostname := lookupEnv("COLLECT_INFLUXDB_HOSTNAME", dryRun)
	database := lookupEnv("COLLECT_INFLUXDB_DATABASE", dryRun)

	cfg := client.HTTPConfig{
		Addr:     hostname,
		Username: username,
		Password: password,
	}

	var c client.Client
	mm := MeterMap{}

	if !dryRun {
		log.Printf("connecting to %q@%q", username, hostname)
		c, err := client.NewHTTPClient(cfg)
		if err != nil {
			log.Fatalln(err)
		}
		defer c.Close()

		mm.Preload(c, database)
		log.Printf("preloaded %d meters\n", len(mm))
	}

	// Store points in the given database with second resolution.
	bpConfig := client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	}

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

		// Create a new batch of points.
		bp, err := client.NewBatchPoints(bpConfig)
		if err != nil {
			log.Println(errors.Wrap(err, "new batch points"))
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

		// Messages know how to add points to a batch.
		msg.AddPoints(logMsg, bp)

		for _, p := range bp.Points() {
			log.Printf("%+v\n", p)
		}

		if !dryRun {
			if err := c.Write(bp); err != nil {
				log.Println(err)
			}
		}
	}
}
