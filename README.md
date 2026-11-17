# go-ntrip

Go NTRIP v2 clients for rovers and correction sources.

Requires Go 1.27.4 or newer. The module has no third-party dependencies.

The rover client is full-duplex: it reads RTCM corrections while keeping the
same caster connection writable for periodic NMEA GGA updates. This is required
by VRS and nearest-base casters and is the reason the package uses raw HTTP/1.1
over `net.Conn`/`tls.Conn` instead of `net/http.Client` for streaming sessions.

## Features

- NTRIP v2 rover `GET` streams with full-duplex GGA writes.
- NTRIP v2 source `POST` streams with chunked transfer encoding.
- HTTPS/TLS with custom `tls.Config` support.
- Basic authentication.
- Bounded sourcetable fetching and tolerant parsing for `CAS`, `NET`, and `STR` records, including parse warnings, raw-record preservation, stream lookup, network lookup, and nearest-stream selection.
- Reconnecting rover sessions and retrying source connection setup with jittered backoff.
- Deadline-bound request/header/write operations and read-idle reconnects for autonomous rover links.
- NMEA GGA formatting, field validation, and checksum helpers.
- Standard-library only.

## Differentiation

Most existing Go NTRIP packages are useful but incomplete for modern rover integrations:

- [`github.com/go-gnss/ntrip`](https://github.com/go-gnss/ntrip) provides a good sourcetable model and warning-oriented parser. Its rover client uses `net/http` and exposes only `resp.Body`, so it cannot send periodic GGA on the same active rover stream.
- [`github.com/de-bkg/gognss/pkg/ntrip`](https://github.com/de-bkg/gognss/tree/main/pkg/ntrip) has a higher-level client API, sourcetable helpers, and caster metadata handling, but its public rover stream shape is read-only.
- [`github.com/bezineb5/ntrip-client`](https://github.com/bezineb5/ntrip-client) explores registry and nearest-station selection on top of `go-gnss/ntrip`; this package includes nearest-stream helpers directly while keeping the transport full-duplex.
- [`github.com/facebook/time/ntrip`](https://github.com/facebook/time/tree/main/ntrip) has a clean source/push client API; this package covers both source `POST` and rover `GET` with GGA.

This package is differentiated by treating NTRIP v2 as an HTTP/1.1-shaped protocol over a bidirectional TCP/TLS stream for rover sessions. The low-level `Rover` remains an `io.Reader` for RTCM and exposes `WriteGGA` for upstream NMEA updates. The managed `RoverStream` adds reconnect/backoff and periodic GGA resend without hiding the raw bytes.

## Production Defaults

- TCP and TLS setup timeout: 10 seconds total.
- Response header timeout: 10 seconds.
- Response header limit: 64 KiB, shared across informational and final responses.
- Sourcetable body timeout: 30 seconds total; decoded body limit: 8 MiB.
- Write timeout: 5 seconds.
- Managed rover read-idle timeout: 60 seconds.
- Nominal reconnect backoff: 500 milliseconds to 30 seconds, factor 2. Each wait
  samples the upper half of that delay (equal jitter).
- Backoff resets only after a session lasts 30 seconds and receives data.

Rover sessions and source connection setup retry transient network errors and
`408`, `425`, `429`, `500`, `502`, `503`, and `504` status responses. Invalid
configuration, malformed protocol responses, certificate verification failures,
and other statuses stop the run. Handler, source-reader, and destination-writer
errors are terminal; they do not trigger caster reconnects.

HTTP/1.0 and HTTP/1.1 responses are parsed with the standard library. Chunked
responses are decoded, short Content-Length bodies report truncation, and only
`200 OK` starts a stream. Canonical legacy `ICY 200 OK\r\n` responses are also
accepted: their payload starts immediately after that line, without HTTP headers.
Content encodings other than identity are unsupported and rejected.
Source uploads require an HTTP/1.1 acknowledgment; legacy ICY/HTTP/1.0 responses
cannot establish acceptance of an HTTP/1.1 chunked request body.

The transport manages Host, Authorization, User-Agent, Ntrip-Version, connection,
and body-framing headers; custom headers cannot override them. Invalid or
case-insensitively duplicated custom headers are rejected. Use `UserAgent` and
`Credentials` configuration fields instead. Basic authentication is unencrypted
over plain HTTP; use HTTPS when credentials require confidentiality.

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

if err := rover.WriteGGA("$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47"); err != nil {
    return err
}

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

`rtcmDestination` must be an `io.WriteCloser` whose `Close` unblocks `Write`.
`RunToWriter` owns and closes it on completion or cancellation. A destination
interrupted by a session failure is terminal, not reused for another connection.
Refresh `SetGGA` with a newly formatted sentence as position/time changes; stored
sentences are retransmitted verbatim. The examples refresh fixed-position time.

For custom processing, `Run` accepts a context-aware callback:

```go
err := stream.Run(ctx, func(ctx context.Context, fragment []byte) error {
    return processCorrections(ctx, fragment)
})
```

Fragments are arbitrary byte ranges, not RTCM message boundaries. The buffer is
borrowed until the callback returns; copy it before retaining it. Callbacks are
serial and must honor their session context if they block. No callback or network
worker outlives `Run`. Only one run per stream is permitted; `Stats` and `SetGGA`
are safe concurrently. Configuration and its referenced maps/configs must not be
mutated after they are supplied to a client or managed stream.

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
if err != nil {
    return err // deferred Close aborts the connection
}
return source.Finish() // sends the final chunk and reports finalization errors
```

`Source.Close` aborts immediately and interrupts blocked writes. `Finish` serializes
with writers, emits the final chunk, and closes the connection. A failed write
poisons that connection; later writes are rejected. Chunked writes use scatter/
gather buffers without copying the RTCM payload into a second framing buffer.

`SourceStream.Run(ctx, src)` takes ownership of an `io.ReadCloser` whose `Close`
unblocks `Read`. It closes the input on return or cancellation and updates byte
statistics as writes complete. Transient connection-setup failures retry before
reading input. Once input is consumed, a write failure returns a `*DeliveryError`
containing the consumed block and payload byte count, rather than silently
discarding or automatically replaying bytes. Use `errors.As` to inspect it. On
cancellation, `errors.Is(err, ctx.Err())` remains true and delivery details are
preserved when a consumed block is involved.

Write counts mean bytes accepted by TCP/TLS, **not caster acknowledgments**. NTRIP
has no per-block delivery acknowledgment. Recovery after an uncertain write must
be decided by the application using its RTCM framing/input policy. Neither a
successful write nor successful `Finish` promises exactly-once delivery.

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

`ParseSourcetable` tolerates field-level problems and records them in `Warnings`.
It requires `ENDSOURCETABLE` and rejects records after the terminator. Missing,
non-finite, or out-of-range coordinates are represented by NaN, warned about, and
excluded from nearest-stream selection. Invalid boolean markers are warned about.
`NearestStreams` uses a stable standard-library sort; ties retain input order.
Invalid query positions/radii return no candidates. `DistanceMeters` returns NaN
for invalid positions and clamps floating-point roundoff near antipodal points.

`String` serializes current typed values without rounding valid coordinates,
preserves extension fields and unknown records, and groups records by type.
`RawString` preserves the original record order and original field text, including
malformed values; it is a snapshot and does not reflect typed edits. Both normalize
line endings to CRLF. `Raw` excludes empty lines and the terminator.
Parsing rejects tables exceeding 100,000 records, 1,000 warnings, or a 1 MiB line
to bound parsed-object amplification from malformed or oversized tables.

## GGA and copy lifecycle

GGA helpers enforce the NMEA 82-byte limit including CRLF, two-letter talkers,
field structure, UTC time, coordinate/minute ranges, fix quality 0–8, satellite
counts 00–99, finite numeric values, meter units, and checksums. Empty position
fields are permitted for an invalid fix. Altitude is mean-sea-level (orthometric)
height; geoid height is ellipsoid minus mean-sea-level height. Formatting rounds
coordinates with carry into the degree field. An optional trailing CRLF is accepted;
embedded line breaks and whitespace surrounding a sentence are rejected.

`PumpRTCM` owns both an `io.ReadCloser` and `io.WriteCloser`, closing both on return
or cancellation. Their Close methods must unblock pending I/O. `PumpGGA` owns its
rover. For arbitrary readers, writers, or callbacks that cannot be interrupted,
Go cannot guarantee bounded cancellation; do not wrap blocking I/O in no-op
closers to satisfy these contracts. Use low-level clients when the application
needs to manage endpoint ownership itself.

## Examples

See [`examples/`](examples/) for buildable programs covering:

- Reliable rover streaming with reconnect and periodic GGA dispatching RTCM to UDP.
- Dispatching RTCM to multiple sinks: UDP, file, stdout, or a raw device path.
- Fetching a sourcetable and selecting the nearest fixed mountpoint.
- Publishing source RTCM to a caster with NTRIP v2 `POST`.
- Formatting and validating fixed-position GGA sentences.
