### Purpose
rtlamr-collect provides data aggregation for rtlamr. This tool in tandem with rtlamr provides easy and accurate data collection for meters which transmit Interval Data Messages (IDM).

### Requirements
 * GoLang >=1.9.2 (Go build environment setup guide: http://golang.org/doc/code.html)
 * rtlamr
 * InfluxDB >=1.4.2

### Building
Downloading and building rtlamr-collect is as easy as:

	go get github.com/bemasher/rtlamr-collect

This will produce the binary `$GOPATH/bin/rtlamr-collect`. For convenience it's common to add `$GOPATH/bin` to the path.

Provisioning influxdb can be done using the included initial schema (assuming you have the influx cli client installed):

	influx -host "host" -username 'user' -password 'pass' -import -path "init.iql"

This creates a database `rtlamr`, four retention policies and associated continuous queries:

 * 240 minutes of 5 minute intervals (used as scratch space prior to deduplication with a continuous query).
 * 1 week of 5 minute intervals.
 * 30 days of 1 hour intervals.
 * 5 years of 1 day intervals.

All data written by rtlamr-collect occurs in the `power` measurement. All aggregated values are the summation of their constituent values.

### Usage
rtlamr-collect is entirely configured through environment variables:
 * `COLLECT_INFLUXDB_HOSTNAME=localhost` InfluxDB hostname to write data to.
 * `COLLECT_INFLUXDB_USER=username` InfluxDB username to authenticate with.
 * `COLLECT_INFLUXDB_PASS=password` InfluxDB password to authenticate with.

At a minimum rtlamr must have the following environment variables defined:
 * `RTLAMR_MSGTYPE=idm` Currently only idm packets are supported.
 * `RTLAMR_FORMAT=json` rtlamr-collect input must be json.
 * `RTLAMR_FILTERID=000000000` List your meter id's here separated by commas. This is not strictly necessary, but it is highly recommended because listening to all the meters in a given area is likely to have high series cardinality and will negatively impact InfluxDB's performance.

rtlamr-collect should take its input directly from the output of an rtlamr instance through a pipe.

```bash
$ rtlamr | rtlamr-collect
```

### Behavior
On startup, rtlamr-collect attempts to pre-load data from InfluxDB to avoid writing duplicate data points between instances.

rtlamr-collect reads IDM packets serialized as json from stdin and for each message determines the transmit offset time and compares this with previously received messages to determine how many new data points (if any) are in the current message. All new data points are written to the `power` measurement in InfluxDB.

rtlamr-collect happily and gracefully collects and aggregates messages from as many meters as it sees on its input.

### Other
Data visualization is left as an exercise for the user. I have had a good experience with grafana, however Chronograf and others should work equally well.

![Grafana Power Usage Dashboard](capture.png "Grafana Power Usage Dashboard")

### Feedback
If you have any general questions or feedback leave a comment below. For bugs, feature suggestions and anything directly relating to the program itself, submit an issue in github.
