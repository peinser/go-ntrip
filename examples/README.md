# Examples

These examples are small, buildable programs that show production-oriented NTRIP v2 usage with only the Go standard library plus `github.com/peinser/go-ntrip`.

## reliable-rover

Maintains a reconnecting rover connection, sends periodic GGA, and dispatches RTCM corrections to a UDP endpoint.

```sh
go run ./examples/reliable-rover \
  -url https://corshub.peinser.com/* \
  -username anonymous \
  -password anonymous \
  -lat 50.85034 \
  -lon 4.35171 \
  -udp 127.0.0.1:13320
```

Use this shape when another local process owns the GNSS receiver or autopilot correction injection path.

## dispatch-sinks

Dispatches one rover RTCM stream to one or more sinks: UDP, file, stdout, and a Unix device path.

```sh
go run ./examples/dispatch-sinks \
  -url https://corshub.peinser.com/* \
  -username rover \
  -password secret \
  -lat 50.85034 \
  -lon 4.35171 \
  -udp 127.0.0.1:13320 \
  -file corrections.rtcm
```

Use `-device /dev/ttyACM0` only when the target accepts raw RTCM bytes directly.

## nearest-mountpoint

Fetches and parses the caster sourcetable, selects the nearest mountpoint, then starts a reliable rover stream from that mountpoint.

```sh
go run ./examples/nearest-mountpoint \
  -caster https://corshub.peinser.com \
  -username rover \
  -password secret \
  -lat 50.85034 \
  -lon 4.35171 \
  -udp 127.0.0.1:13320
```

For CORSHub, mountpoint `*` can be preferable because the caster can choose dynamically from GGA. This example is useful for casters where the client must pick a fixed mountpoint.

## source-publisher

Publishes RTCM bytes from stdin or a file to a caster using NTRIP v2 `PUT`.

```sh
go run ./examples/source-publisher \
  -url https://corshub.peinser.com/BASE-01 \
  -username base \
  -password secret \
  -file base.rtcm
```

Omit `-file` to read from stdin.

## fixed-gga

Shows how to generate and validate NMEA GGA sentences for fixed-position rover updates.

```sh
go run ./examples/fixed-gga -lat 50.85034 -lon 4.35171 -alt 65.4
```
