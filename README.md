# go-ntrip

Go NTRIP v2 clients for rovers and correction sources.

The rover client is full-duplex: it reads RTCM corrections while keeping the
same caster connection writable for periodic NMEA GGA updates. This is required
by VRS and nearest-base casters and is the reason the package uses raw HTTP/1.1
over `net.Conn`/`tls.Conn` instead of `net/http.Client` for streaming sessions.

## Features

- NTRIP v2 rover `GET` streams with full-duplex GGA writes.
- NTRIP v2 source `PUT` streams with chunked transfer encoding by default.
- HTTPS/TLS with custom `tls.Config` support.
- Basic authentication.
- Sourcetable fetch and parsing for `CAS`, `NET`, and `STR` records, including parse warnings, round-tripping, stream lookup, network lookup, and nearest-stream selection.
- Managed rover and source stream abstractions with reconnect/backoff.
- Deadline-bound request/header/write operations and read-idle reconnects for autonomous rover links.
- NMEA GGA formatting and checksum validation helpers.
- Standard-library only.

## Differentiation

Most existing Go NTRIP packages are useful but incomplete for modern rover integrations:

- [`github.com/go-gnss/ntrip`](https://github.com/go-gnss/ntrip) provides a good sourcetable model and warning-oriented parser. Its rover client uses `net/http` and exposes only `resp.Body`, so it cannot send periodic GGA on the same active rover stream.
- [`github.com/de-bkg/gognss/pkg/ntrip`](https://github.com/de-bkg/gognss/tree/main/pkg/ntrip) has a higher-level client API, sourcetable helpers, and caster metadata handling, but its public rover stream shape is read-only.
- [`github.com/bezineb5/ntrip-client`](https://github.com/bezineb5/ntrip-client) explores registry and nearest-station selection on top of `go-gnss/ntrip`; this package includes nearest-stream helpers directly while keeping the transport full-duplex.
- [`github.com/facebook/time/ntrip`](https://github.com/facebook/time/tree/main/ntrip) has a clean source/push client API; this package covers both source `PUT` and rover `GET` with GGA.

This package is differentiated by treating NTRIP v2 as an HTTP/1.1-shaped protocol over a bidirectional TCP/TLS stream for rover sessions. The low-level `Rover` remains an `io.Reader` for RTCM and exposes `WriteGGA` for upstream NMEA updates. The managed `RoverStream` adds reconnect/backoff and periodic GGA resend without hiding the raw bytes.

## Production Defaults

- Dial timeout: 10 seconds.
- Response header timeout: 10 seconds.
- Write timeout: 5 seconds.
- Rover read-idle timeout: 60 seconds.
- Reconnect backoff: 500 milliseconds to 30 seconds, factor 2.

Managed streams retry transient network and server failures. They stop on invalid local configuration and non-transient status responses such as `401 Unauthorized`, so supervisors can surface real operator action items instead of looping forever.

## Rover

```go
rover, err := ntrip.DialRover(ctx, ntrip.RoverConfig{
    URL: "https://corshub.peinser.com/*",
    Credentials: ntrip.Credentials{Username: "rover", Password: "secret"},
})
if err != nil {
    return err
}
defer rover.Close()

go func() {
    _ = rover.WriteGGA("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47")
}()

_, err = io.Copy(rtcmDestination, rover)
```

## Managed Rover Stream

```go
stream := ntrip.NewRoverStream(ntrip.StreamConfig{
    Rover: ntrip.RoverConfig{
        URL: "https://corshub.peinser.com/*",
        Credentials: ntrip.Credentials{Username: "rover", Password: "secret"},
    },
    GGAInterval: 5 * time.Second,
    ReconnectOnEOF: true,
})

if err := stream.SetGGA("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47"); err != nil {
    return err
}
err := stream.RunToWriter(ctx, rtcmDestination)
```

## Source

```go
source, err := ntrip.DialSource(ctx, ntrip.SourceConfig{
    URL: "https://corshub.peinser.com/BASE-01",
    Credentials: ntrip.Credentials{Username: "base", Password: "secret"},
})
if err != nil {
    return err
}
defer source.Close()

_, err = io.Copy(source, rtcmSource)
```

## Sourcetable

```go
table, err := ntrip.FetchSourcetable(ctx, ntrip.SourcetableConfig{
    URL: "https://corshub.peinser.com",
    Credentials: ntrip.Credentials{Username: "anonymous", Password: "anonymous"},
})
if err != nil {
    return err
}

nearest := table.NearestStreams(50.85034, 4.35171, 50_000)
```

`ParseSourcetable` is tolerant: it returns parsed records and stores row-level issues in `Sourcetable.Warnings` instead of discarding the entire table for one malformed field.

## Examples

See [`examples/`](examples/) for buildable programs covering:

- Reliable rover streaming with reconnect and periodic GGA dispatching RTCM to UDP.
- Dispatching RTCM to multiple sinks: UDP, file, stdout, or a raw device path.
- Fetching a sourcetable and selecting the nearest fixed mountpoint.
- Publishing source RTCM to a caster with NTRIP v2 `PUT`.
- Formatting and validating fixed-position GGA sentences.
