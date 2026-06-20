// Package ntrip implements NTRIP v2 clients for rovers and correction sources.
//
// The package intentionally uses raw HTTP/1.1 over net.Conn/tls.Conn instead of
// net/http.Client for rover streams. Many NTRIP casters expect a rover to keep
// the connection writable after the response starts so it can periodically send
// NMEA GGA position updates while reading RTCM corrections. net/http exposes the
// response body as a read-only stream, which is insufficient for that full-duplex
// rover use case.
package ntrip
