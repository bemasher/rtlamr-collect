## Installation
Copy the provided units and environment files to `/etc/systemd/system`. Follow the configuration instructions below.

Enable and start the units:
```
systemctl enable rtl_tcp rtlamr-collect
systemctl start rtl_tcp rtlamr-collect
```

### Configuration
At minimum, change the following variables according to your setup:

 * `GOPATH` Set because `GOPATH` is usually user-specific and won't be available to the service.
 * `RTLAMR_FILTERID`
 * `COLLECT_INFLUXDB_HOSTNAME`
 * `COLLECT_INFLUXDB_USER`
 * `COLLECT_INFLUXDB_PASS`

For more information on these environment variables, see the main project's readme.