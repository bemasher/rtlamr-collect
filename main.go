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

	"github.com/bemasher/rtlamr/crc"
	"github.com/influxdata/influxdb/client/v2"
)

const (
	dbName = "rtlamr"
	host   = "http://%s:8086"

	threshold = 30 * time.Second
)

type Message struct {
	Time time.Time `json:"Time"`
	IDM  IDM       `json:"Message"`
}

type IDM struct {
	PacketTypeID       uint8    `json:"PacketTypeID"`
	PacketLength       uint8    `json:"PacketLength"`
	HammingCode        uint8    `json:"HammingCode"`
	ApplicationVersion uint8    `json:"ApplicationVersion"`
	EndPointType       byte     `json:"ERTType"`
	EndPointID         uint32   `json:"ERTSerialNumber"`
	Consumption        uint32   `json:"LastConsumptionCount"`
	TransmitTime       uint16   `json:"TransmitTimeOffset"`
	IntervalCount      byte     `json:"ConsumptionIntervalCount"`
	Intervals          []uint16 `json:"DifferentialConsumptionIntervals"`
	IDCRC              uint16   `json:"SerialNumberCRC"`
}

func (idm IDM) Tags(idx int) map[string]string {
	return map[string]string{
		"endpoint_id": strconv.Itoa(int(idm.EndPointID)),
	}
}

func (idm IDM) Fields(idx int) map[string]interface{} {
	return map[string]interface{}{
		"consumption": float64(idm.Intervals[idx]) * 10,
		"interval":    int64(uint(int(idm.IntervalCount)-idx) % 256),
	}
}

type Consumption struct {
	New   [256]bool
	Time  [256]time.Time
	Usage [256]uint16
}

func (c Consumption) Fields(idx uint) map[string]interface{} {
	return map[string]interface{}{
		"consumption": float64(c.Usage[idx]),
	}
}

func (c *Consumption) Update(msg Message) {
	for idx := range c.New {
		c.New[idx] = false
	}

	timeOffset := time.Duration(msg.IDM.TransmitTime) * 62500 * time.Microsecond
	for idx, usage := range msg.IDM.Intervals {
		interval := uint(int(msg.IDM.IntervalCount)-idx) % 256
		t := msg.Time.Add(-time.Duration(idx)*5*time.Minute - timeOffset).Truncate(time.Second)

		diff := c.Time[interval].Sub(t)
		if c.Time[interval].IsZero() || diff > threshold || diff < -threshold {
			c.New[interval] = true
			c.Time[interval] = t
			c.Usage[interval] = usage * 10
		}
	}
}

type MeterMap map[uint32]Consumption

func (mm MeterMap) Preload(c client.Client) {
	q := client.NewQuery("SELECT * FROM power WHERE time > now() - 4h", "distinct", "ns")
	if res, err := c.Query(q); err == nil && res.Error() == nil {
		for _, r := range res.Results {
			for _, s := range r.Series {
				for _, v := range s.Values {
					// time, usage, endpoint_id, endpoint_type, interval
					nsec, _ := v[0].(json.Number).Int64()
					usage, _ := v[1].(json.Number).Int64()
					interval, _ := v[2].(json.Number).Int64()
					id, _ := strconv.Atoi(v[3].(string))
					meter := uint32(id)

					if _, exists := mm[meter]; !exists {
						mm[meter] = Consumption{}
					}

					consumption := mm[meter]
					consumption.Time[interval] = time.Unix(0, nsec)
					consumption.Usage[interval] = uint16(usage)
					mm[meter] = consumption
				}
			}
		}
	}

	log.Printf("Preloaded: %d", len(mm))
}

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	stdinBuf := bufio.NewScanner(os.Stdin)

	hostname, ok := os.LookupEnv("COLLECT_INFLUXDB_HOSTNAME")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_HOSTNAME undefined")
	}

	username, ok := os.LookupEnv("COLLECT_INFLUXDB_USER")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_USER undefined")
	}

	password, ok := os.LookupEnv("COLLECT_INFLUXDB_PASS")
	if !ok {
		log.Fatal("COLLECT_INFLUXDB_PASS undefined")
	}

	log.Printf("connecting to %q@%q with %q", username, fmt.Sprintf(host, hostname), password)
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     fmt.Sprintf(host, hostname),
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatalln(err)
	}

	batchPointConfig := client.BatchPointsConfig{
		Database:  dbName,
		Precision: "s",
	}

	idmCRC := crc.NewCRC("CCITT", 0xFFFF, 0x1021, 0x1D0F)

	mm := make(MeterMap)
	mm.Preload(c)

	for stdinBuf.Scan() {
		var msg Message
		err := json.Unmarshal(stdinBuf.Bytes(), &msg)
		if err != nil {
			log.Println(err)
			continue
		}
		idm := msg.IDM

		buf := make([]byte, 6)
		binary.BigEndian.PutUint32(buf[:4], msg.IDM.EndPointID)
		binary.BigEndian.PutUint16(buf[4:], msg.IDM.IDCRC)
		if residue := idmCRC.Checksum(buf); residue != idmCRC.Residue {
			continue
		}

		bp, err := client.NewBatchPoints(batchPointConfig)
		if err != nil {
			log.Println(err)
			continue
		}

		if _, exists := mm[idm.EndPointID]; !exists {
			mm[idm.EndPointID] = Consumption{}
		}

		consumption := mm[idm.EndPointID]
		consumption.Update(msg)
		mm[idm.EndPointID] = consumption

		for idx := range idm.Intervals {
			interval := uint(int(msg.IDM.IntervalCount)-idx) % 256

			if !consumption.New[interval] {
				continue
			}

			pt, err := client.NewPoint(
				"power",
				idm.Tags(idx),
				consumption.Fields(interval),
				consumption.Time[interval],
			)

			if err != nil {
				log.Println(err)
			} else {
				bp.AddPoint(pt)
			}
		}

		if err := c.Write(bp); err != nil {
			log.Println(err)
		}
	}
}
