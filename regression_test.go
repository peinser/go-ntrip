package ntrip

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker")
		var zero T
		return zero
	}
}

func serveOnce(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	ln := listenTCP(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
		defer stop()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		handle(conn)
	}()
	t.Cleanup(func() { cancel(); _ = ln.Close(); receive(t, done) })
	return "http://" + ln.Addr().String() + "/MOUNT"
}

func TestRoverHTTPFraming(t *testing.T) {
	for _, tt := range []struct {
		name, wire, want             string
		wantDialError, wantBodyError bool
	}{
		{"chunked with trailer", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n4\r\nRTCM\r\n0\r\nX-End: yes\r\n\r\n", "RTCM", false, false},
		{"content length", "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nRTCMignored", "RTCM", false, false},
		{"informational responses", "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nRTCM", "RTCM", false, false},
		{"legacy headerless ICY", "ICY 200 OK\r\n\xd3\x00\x01B", "\xd3\x00\x01B", false, false},
		{"short content length", "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nRTCM", "RTCM", false, true},
		{"missing chunk terminator", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nRTCM\r\n", "RTCM", false, true},
		{"missing trailer terminator", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nRTCM\r\n0\r\n", "RTCM", false, true},
		{"unsupported transfer encoding", "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", "", true, false},
		{"unsupported content encoding", "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n", "", true, false},
		{"multiple content encodings", "HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\nContent-Encoding: gzip\r\n\r\n", "", true, false},
		{"conflicting lengths", "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 8\r\n\r\n", "", true, false},
		{"protocol upgrade", "HTTP/1.1 101 Switching Protocols\r\n\r\n", "", true, false},
		{"no content is not a stream", "HTTP/1.1 204 No Content\r\n\r\n", "", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			url := serveOnce(t, func(conn net.Conn) {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err == nil {
					_, _ = io.WriteString(conn, tt.wire)
				}
			})
			rover, err := DialRover(context.Background(), RoverConfig{URL: url})
			if tt.wantDialError {
				if err == nil {
					_ = rover.Close()
					t.Fatal("expected dial rejection")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer rover.Close()
			body, err := io.ReadAll(rover)
			if string(body) != tt.want || (err != nil) != tt.wantBodyError {
				t.Fatalf("body=%q, error=%v", body, err)
			}
		})
	}
}

func TestChunkedRoverRemainsFullDuplex(t *testing.T) {
	gga := make(chan string, 1)
	url := serveOnce(t, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		if _, err := http.ReadRequest(reader); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nR\r\n")
		line, _ := reader.ReadString('\n')
		gga <- line
		_, _ = io.WriteString(conn, "1\r\nT\r\n0\r\n\r\n")
	})
	rover, err := DialRover(context.Background(), RoverConfig{URL: url})
	if err != nil {
		t.Fatal(err)
	}
	defer rover.Close()
	first := make([]byte, 1)
	if _, err := io.ReadFull(rover, first); err != nil || string(first) != "R" {
		t.Fatalf("first=%q, %v", first, err)
	}
	if err := rover.WriteGGA(testGGA); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, gga); got != testGGA+"\r\n" {
		t.Fatalf("GGA=%q", got)
	}
	rest, err := io.ReadAll(rover)
	if err != nil || string(rest) != "T" {
		t.Fatalf("rest=%q, %v", rest, err)
	}
}

type countingReader struct {
	io.Reader
	n int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += n
	return n, err
}

func TestHeaderLimitBoundsConsumption(t *testing.T) {
	for _, prefix := range []string{"", "HTTP/1.1 200 OK\r\nX-Large: "} {
		raw := &countingReader{Reader: strings.NewReader(prefix + strings.Repeat("x", 1<<20))}
		reader := bufio.NewReaderSize(raw, 32)
		_, err := readResponse(reader, 64)
		if !errors.Is(err, ErrBadResponse) {
			t.Fatalf("error=%v", err)
		}
		if raw.n > 64+reader.Size() {
			t.Fatalf("consumed %d bytes for a 64-byte limit", raw.n)
		}
	}
	_, err := readResponse(bufio.NewReader(strings.NewReader("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n\r\n")), 40)
	if !errors.Is(err, ErrBadResponse) {
		t.Fatalf("informational headers bypassed shared limit: %v", err)
	}
}

func TestInvalidRequestsRejectedBeforeDial(t *testing.T) {
	for _, tt := range []struct {
		name, agent, username string
		headers               map[string]string
	}{
		{name: "agent injection", agent: "NTRIP test\r\nX-Injected: yes"},
		{name: "header name whitespace", headers: map[string]string{"Bad Name": "value"}},
		{name: "header value control", headers: map[string]string{"X-Test": "value\x00"}},
		{name: "duplicate spelling", headers: map[string]string{"X-Test": "a", "x-test": "b"}},
		{name: "framing override", headers: map[string]string{"transfer-encoding": "gzip"}},
		{name: "content length", headers: map[string]string{"Content-Length": "1"}},
		{name: "authorization override", headers: map[string]string{"Authorization": "anything"}},
		{name: "colon in username", username: "user:extra"},
		{name: "invalid initial GGA", headers: map[string]string{"Ntrip-GGA": "$GPGGA,bad*00"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := RoverConfig{URL: "http://127.0.0.1:1/MOUNT", UserAgent: tt.agent, Credentials: Credentials{Username: tt.username}, Headers: tt.headers}
			_, err := DialRover(context.Background(), cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error=%v; expected local rejection before network I/O", err)
			}
		})
	}
}

func TestEndpointValidation(t *testing.T) {
	for _, tt := range []struct{ url, address string }{
		{"http://[::1]/MOUNT", "[::1]:2101"},
		{"https://[2001:db8::1]/MOUNT", "[2001:db8::1]:443"},
		{"http://[::1]:2201/MOUNT", "[::1]:2201"},
		{"http://example.com/MOUNT", "example.com:2101"},
	} {
		ep, err := parseEndpoint(tt.url)
		if err != nil || ep.Address != tt.address {
			t.Fatalf("%s: endpoint=%+v, error=%v", tt.url, ep, err)
		}
	}
	for _, url := range []string{"http://example.com:0/", "http://example.com:65536/", "http://example.com/path#fragment", "http://example.com/?q=raw space"} {
		if _, err := parseEndpoint(url); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%s: error=%v", url, err)
		}
	}
}

func TestTLSSetupTimeoutIsTransient(t *testing.T) {
	url := serveOnce(t, func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	_, err := DialRover(ctx, RoverConfig{URL: strings.Replace(url, "http:", "https:", 1), DialTimeout: 20 * time.Millisecond})
	if !errors.Is(err, os.ErrDeadlineExceeded) || !Temporary(err) || ctx.Err() != nil {
		t.Fatalf("error=%v, transient=%v, parent=%v", err, Temporary(err), ctx.Err())
	}
	if time.Since(started) > time.Second {
		t.Fatal("TLS handshake escaped setup timeout")
	}
}

func TestTLSOnlyNegotiatesHTTP1(t *testing.T) {
	protocol := make(chan string, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocol <- r.TLS.NegotiatedProtocol
		_, _ = io.WriteString(w, "ENDSOURCETABLE\r\n")
	}))
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{NextProtos: []string{"h2", "http/1.1"}}
	server.StartTLS()
	defer server.Close()
	cfg := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}
	if _, err := FetchSourcetable(context.Background(), SourcetableConfig{URL: server.URL, TLSConfig: cfg}); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, protocol); got != "http/1.1" {
		t.Fatalf("negotiated %q", got)
	}
	if cfg.NextProtos[0] != "h2" {
		t.Fatal("mutated caller TLS config")
	}
	_, err := FetchSourcetable(context.Background(), SourcetableConfig{URL: server.URL})
	if err == nil || Temporary(err) {
		t.Fatalf("certificate error=%v, transient=%v", err, Temporary(err))
	}
}

type faultConn struct {
	net.Conn
	output    bytes.Buffer
	budget    int
	failure   error
	zeroWrite bool
	closes    int
	closeErr  error
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.zeroWrite {
		return 0, nil
	}
	if c.failure != nil {
		left := c.budget - c.output.Len()
		if left <= 0 {
			return 0, c.failure
		}
		if left < len(p) {
			_, _ = c.output.Write(p[:left])
			return left, c.failure
		}
	}
	return c.output.Write(p)
}
func (c *faultConn) Close() error                     { c.closes++; return c.closeErr }
func (c *faultConn) SetWriteDeadline(time.Time) error { return nil }

func TestSourcePartialWritesPoisonConnection(t *testing.T) {
	failure := errors.New("write failed")
	for _, budget := range []int{0, 1, 3, 5, 7, 8} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			conn := &faultConn{budget: budget, failure: failure}
			source := &Source{conn: conn, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
			n, err := source.Write([]byte("RTCM"))
			want := max(0, min(4, budget-3))
			if n != want || !errors.Is(err, failure) {
				t.Fatalf("write=%d,%v; want %d", n, err, want)
			}
			before := conn.output.String()
			if n, err := source.Write([]byte("next")); n != 0 || !errors.Is(err, failure) {
				t.Fatalf("second write=%d,%v", n, err)
			}
			if err := source.Finish(); !errors.Is(err, failure) {
				t.Fatalf("finish=%v", err)
			}
			if conn.output.String() != before || conn.closes != 1 {
				t.Fatalf("corrupted failed stream or closed %d times", conn.closes)
			}
		})
	}
}

func TestSourceFinishAndRawWriteCounts(t *testing.T) {
	conn := &faultConn{}
	source := &Source{conn: conn, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
	if n, err := source.Write([]byte("RTCM")); n != 4 || err != nil {
		t.Fatalf("write=%d,%v", n, err)
	}
	if err := source.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := source.Finish(); err != nil {
		t.Fatal(err)
	}
	if got := conn.output.String(); got != "4\r\nRTCM\r\n0\r\n\r\n" {
		t.Fatalf("wire=%q", got)
	}
	if n, err := source.Write([]byte("late")); n != 0 || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("late write=%d,%v", n, err)
	}
	if conn.closes != 1 {
		t.Fatalf("closes=%d", conn.closes)
	}
	failure := errors.New("partial raw write")
	broken := &faultConn{budget: 2, failure: failure}
	rover := &Rover{conn: broken, closeConn: closeWithContext(context.Background(), broken), writeTimeout: time.Second}
	if n, err := rover.Write([]byte("RTCM")); n != 2 || !errors.Is(err, failure) {
		t.Fatalf("raw write=%d,%v", n, err)
	}
	if _, err := writeFull(&faultConn{zeroWrite: true}, []byte("RTCM"), time.Second); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero-progress write=%v", err)
	}
	finishConn := &faultConn{budget: 9, failure: failure}
	unfinished := &Source{conn: finishConn, closeConn: closeWithContext(context.Background(), finishConn), writeTimeout: time.Second}
	if _, err := unfinished.Write([]byte("RTCM")); err != nil {
		t.Fatal(err)
	}
	if err := unfinished.Finish(); !errors.Is(err, failure) {
		t.Fatalf("lost final-chunk error: %v", err)
	}
	if err := unfinished.Finish(); !errors.Is(err, failure) || finishConn.closes != 1 {
		t.Fatalf("repeated finish=%v, closes=%d", err, finishConn.closes)
	}
}

func TestSourceStreamPOSTAndGracefulEOF(t *testing.T) {
	result := make(chan string, 1)
	url := serveOnce(t, func(conn net.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			result <- err.Error()
			return
		}
		if req.Method != "POST" || len(req.TransferEncoding) != 1 || req.TransferEncoding[0] != "chunked" {
			result <- "invalid request method/framing"
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
		body, err := io.ReadAll(req.Body)
		if err != nil {
			result <- err.Error()
			return
		}
		result <- string(body)
	})
	stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: url}})
	if err := stream.Run(context.Background(), io.NopCloser(strings.NewReader("RTCM"))); err != nil {
		t.Fatal(err)
	}
	if got := receive(t, result); got != "RTCM" {
		t.Fatalf("body=%q", got)
	}
	if stats := stream.Stats(); stats.BytesWritten != 4 || stats.LastError != nil {
		t.Fatalf("stats=%+v", stats)
	}
}

type liveInput struct {
	io.ReadCloser
	data     []byte
	nextRead chan struct{}
	once     sync.Once
}

func (r *liveInput) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	r.once.Do(func() { close(r.nextRead) })
	return r.ReadCloser.Read(p)
}

func TestSourceStreamLiveStatsAndCancellation(t *testing.T) {
	url := serveOnce(t, func(conn net.Conn) {
		req, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
		_, _ = io.Copy(io.Discard, req.Body)
	})
	reader, writer := io.Pipe()
	defer writer.Close()
	input := &liveInput{ReadCloser: reader, data: []byte("RTCM"), nextRead: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: url}})
	done := make(chan error, 1)
	go func() { done <- stream.Run(ctx, input) }()
	receive(t, input.nextRead)
	if stats := stream.Stats(); stats.BytesWritten != 4 {
		t.Fatalf("live stats=%+v", stats)
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if _, err := writer.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("input not closed: %v", err)
	}
}

func TestSourceCloseInterruptsBlockedChunkWrite(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	source := &Source{conn: client, closeConn: closeWithContext(context.Background(), client), writeTimeout: time.Hour}
	done := make(chan error, 1)
	go func() { _, err := source.Write([]byte("RTCM")); done <- err }()
	// The peer never reads: even writing a chunk header blocks.
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, done); err == nil {
		t.Fatal("blocked write unexpectedly succeeded")
	}
}

type gatedInput struct {
	io.Reader
	gate   chan struct{}
	closed <-chan struct{}
	once   sync.Once
}

func (r *gatedInput) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.gate); <-r.closed })
	return r.Reader.Read(p)
}
func (r *gatedInput) Close() error { return nil }

func TestSourceWriteFailurePreservesConsumedBlock(t *testing.T) {
	gate, closed := make(chan struct{}), make(chan struct{})
	url := serveOnce(t, func(conn net.Conn) {
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
		<-gate
		_ = conn.(*net.TCPConn).SetLinger(0)
		_ = conn.Close()
		close(closed)
	})
	input := &gatedInput{Reader: strings.NewReader(strings.Repeat("R", 1<<20)), gate: gate, closed: closed}
	stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: url}, BufferSize: 64 * 1024})
	err := stream.Run(context.Background(), input)
	var delivery *DeliveryError
	if !errors.As(err, &delivery) {
		t.Fatalf("error=%v, expected DeliveryError", err)
	}
	if len(delivery.Data) == 0 || delivery.Written < 0 || delivery.Written > len(delivery.Data) || strings.Trim(string(delivery.Data), "R") != "" {
		t.Fatalf("delivery=%+v", delivery)
	}
	if Temporary(err) {
		t.Fatal("uncertain delivery must not automatically retry")
	}
	if stream.Stats().Connections != 1 {
		t.Fatal("reconnected after consuming input")
	}
}

type blockingSink struct {
	entered         chan struct{}
	closed          chan struct{}
	closes          atomic.Int32
	completeOnClose bool
}

func (w *blockingSink) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.closed
	if w.completeOnClose {
		return len(p), nil
	}
	return 0, net.ErrClosed
}
func (w *blockingSink) Close() error {
	if w.closes.Add(1) == 1 {
		close(w.closed)
	}
	return nil
}

func TestManagedWriterAndPumpCancelBlockedWrite(t *testing.T) {
	for _, pump := range []bool{false, true} {
		t.Run(fmt.Sprint(pump), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sink := &blockingSink{entered: make(chan struct{}), closed: make(chan struct{})}
			done := make(chan error, 1)
			if pump {
				go func() { _, err := PumpRTCM(ctx, sink, io.NopCloser(strings.NewReader("RTCM")), nil); done <- err }()
			} else {
				url := serveOnce(t, func(conn net.Conn) {
					if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
						return
					}
					_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\nRTCM")
					_, _ = io.Copy(io.Discard, conn)
				})
				go func() { done <- NewRoverStream(StreamConfig{Rover: RoverConfig{URL: url}}).RunToWriter(ctx, sink) }()
			}
			receive(t, sink.entered)
			cancel()
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			if got := sink.closes.Load(); got != 1 {
				t.Fatalf("closed %d times", got)
			}
		})
	}
}

func TestSessionFailureDoesNotReuseDestinationAfterSuccessfulWriteRace(t *testing.T) {
	sink := &blockingSink{entered: make(chan struct{}), closed: make(chan struct{}), completeOnClose: true}
	url := serveOnce(t, func(conn net.Conn) {
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\nRTCM")
		<-sink.entered
		_ = conn.(*net.TCPConn).SetLinger(0)
		_ = conn.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream := NewRoverStream(StreamConfig{Rover: RoverConfig{URL: url}, GGAInterval: time.Millisecond, ReconnectOnEOF: true})
	if err := stream.SetGGA(testGGA); err != nil {
		t.Fatal(err)
	}
	err := stream.RunToWriter(ctx, sink)
	if err == nil || ctx.Err() != nil {
		t.Fatalf("did not stop on session failure: %v, context=%v", err, ctx.Err())
	}
	if stream.Stats().Connections != 1 || sink.closes.Load() != 1 {
		t.Fatalf("reused destination: stats=%+v, closes=%d", stream.Stats(), sink.closes.Load())
	}
}

type shortSink struct{ closed bool }

func (*shortSink) Write(p []byte) (int, error) { return len(p) - 1, nil }
func (s *shortSink) Close() error              { s.closed = true; return nil }

func TestHandlerAndDestinationErrorsAreTerminal(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			url := serveOnce(t, func(conn net.Conn) {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err == nil {
					_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\nRTCM")
				}
			})
			stream := NewRoverStream(StreamConfig{Rover: RoverConfig{URL: url}, ReconnectOnEOF: true})
			var err error
			if short {
				sink := &shortSink{}
				err = stream.RunToWriter(context.Background(), sink)
				if !sink.closed || !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("closed=%v, error=%v", sink.closed, err)
				}
			} else {
				err = stream.Run(context.Background(), func(context.Context, []byte) error { return io.ErrUnexpectedEOF })
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal(err)
				}
			}
			if stream.Stats().Connections != 1 {
				t.Fatal("retried local failure")
			}
		})
	}
}

type failGGAConn struct {
	net.Conn
	failure error
}

func (c *failGGAConn) Write([]byte) (int, error) { return 0, c.failure }

func TestGGAWriteFailureJoinsSessionWorkers(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	failure := errors.New("GGA write failure")
	conn := &failGGAConn{Conn: client, failure: failure}
	rover := &Rover{conn: conn, reader: client, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
	stream := NewRoverStream(StreamConfig{GGAInterval: time.Millisecond})
	entered, exited := make(chan struct{}), make(chan struct{})
	// Install GGA from the callback so its cancellation necessarily happens
	// while that callback owns the reusable read buffer.
	done := make(chan error, 1)
	go func() {
		err, _ := stream.runConnected(context.Background(), rover, make([]byte, 16), func(ctx context.Context, p []byte) error {
			close(entered)
			_ = stream.SetGGA(testGGA)
			<-ctx.Done()
			defer close(exited)
			if !errors.Is(context.Cause(ctx), failure) {
				return fmt.Errorf("unexpected cancellation cause: %v", context.Cause(ctx))
			}
			p[0] = 'X'
			return ctx.Err()
		})
		done <- err
	}()
	if _, err := server.Write([]byte("R")); err != nil {
		t.Fatal(err)
	}
	receive(t, entered)
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("callback outlived connected session")
	}
}

func TestBackoffGrowsAcrossShortSessions(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 4; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			_, err = http.ReadRequest(bufio.NewReader(conn))
			if err == nil {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
				if i == 3 {
					_, _ = io.WriteString(conn, "R")
				}
			}
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var logs bytes.Buffer
	stream := NewRoverStream(StreamConfig{Rover: RoverConfig{URL: "http://" + ln.Addr().String() + "/MOUNT"}, ReconnectOnEOF: true,
		Reconnect: BackoffConfig{Min: time.Millisecond, Max: 8 * time.Millisecond, ResetAfter: time.Hour}, Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	err := stream.Run(ctx, func(context.Context, []byte) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	receive(t, done)
	decoder := json.NewDecoder(&logs)
	for _, want := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond} {
		var record struct {
			Backoff int64 `json:"backoff"`
		}
		if err := decoder.Decode(&record); err != nil || record.Backoff != int64(want) {
			t.Fatalf("record=%+v, want=%s, error=%v", record, want, err)
		}
	}
}

func TestRetryClassification(t *testing.T) {
	for _, code := range []int{408, 425, 429, 500, 502, 503, 504} {
		if !Temporary(&StatusError{Code: code}) {
			t.Fatalf("should retry %d", code)
		}
	}
	for _, code := range []int{200, 301, 400, 401, 403, 404, 501, 505} {
		if Temporary(&StatusError{Code: code}) {
			t.Fatalf("should not retry %d", code)
		}
	}
	for _, err := range []error{errors.New("disk failed"), context.Canceled, context.DeadlineExceeded, ErrInvalidConfig, fmt.Errorf("%w: malformed header", ErrBadResponse)} {
		if Temporary(err) {
			t.Fatalf("should not retry %v", err)
		}
	}
	if !Temporary(fmt.Errorf("%w: %w", ErrBadResponse, io.ErrUnexpectedEOF)) {
		t.Fatal("truncated header should be retryable")
	}
	reset := fmt.Errorf("%w: %w", ErrBadResponse, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET})
	if !Temporary(reset) {
		t.Fatal("a network reset during headers should remain retryable")
	}
	for _, factor := range []float64{math.NaN(), math.Inf(1), -1} {
		cfg := (BackoffConfig{Factor: factor}).normalized()
		if !finite(cfg.Factor) || cfg.Factor < 1.1 {
			t.Fatalf("invalid normalized factor: %v", cfg.Factor)
		}
	}
	if got := nextDelay(time.Duration(math.MaxInt64-1), BackoffConfig{Max: time.Duration(math.MaxInt64), Factor: 2}); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflowed backoff: %v", got)
	}
}

func TestSourcetableDownloadBounds(t *testing.T) {
	for _, tt := range []struct {
		name, wire string
		limit      int64
		timeout    time.Duration
		hold       bool
	}{
		{name: "truncated length", wire: "HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\nENDSOURCETABLE\r\n"},
		{name: "missing table terminator", wire: "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nSTR;"},
		{name: "body too large", wire: "HTTP/1.1 200 OK\r\n\r\n" + strings.Repeat("x", 100), limit: 32},
		{name: "body timeout", wire: "HTTP/1.1 200 OK\r\n\r\n", timeout: 20 * time.Millisecond, hold: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			url := serveOnce(t, func(conn net.Conn) {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				_, _ = io.WriteString(conn, tt.wire)
				if tt.hold {
					_, _ = io.Copy(io.Discard, conn)
				}
			})
			_, err := FetchSourcetable(context.Background(), SourcetableConfig{URL: url, BodyTimeout: tt.timeout, MaxBodyBytes: tt.limit})
			if err == nil {
				t.Fatal("accepted invalid/incomplete sourcetable")
			}
		})
	}
}

func TestSourcetablePreservesRecordsAndCoordinatePrecision(t *testing.T) {
	input := "STR;BASE;Base;RTCM 3.3;1005;2;GPS;NET;BEL;50.85034123456789;4.3517123456789;0;0;GEN;none;B;N;9600;misc;extension\r\n" +
		"XYZ;unknown;record\r\nNET;NET;Operator;B;N;url;url;email;misc;extra\r\nENDSOURCETABLE\r\n"
	table, err := ParseSourcetable(input)
	if err != nil {
		t.Fatal(err)
	}
	if got := table.RawString(); got != input {
		t.Fatalf("raw round trip=%q", got)
	}
	roundTrip, err := ParseSourcetable(table.String())
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.Streams[0].Latitude != table.Streams[0].Latitude || roundTrip.Streams[0].Longitude != table.Streams[0].Longitude || roundTrip.Streams[0].Extra[0] != "extension" || roundTrip.Networks[0].Extra[0] != "extra" || roundTrip.Unknown[0] != "XYZ;unknown;record" {
		t.Fatal("typed round trip lost data")
	}
	table.Streams[0].Identifier = "Edited"
	if !strings.Contains(table.String(), ";Edited;") || table.RawString() != input {
		t.Fatal("raw snapshot/typed serialization semantics incorrect")
	}
	if _, err := ParseSourcetable("ENDSOURCETABLE\nSTR;late\n"); err == nil {
		t.Fatal("accepted records after terminator")
	}
}

func TestNearestStreamsRejectInvalidCoordinatesAndKeepTies(t *testing.T) {
	row := "STR;%s;Base;RTCM 3.3;1005;2;GPS;NET;BEL;%s;%s;0;0;GEN;none;B;N;9600;misc\n"
	input := fmt.Sprintf(row, "first", "0", "0") + fmt.Sprintf(row, "second", "0", "0") +
		fmt.Sprintf(row, "invalid", "bad", "0") + fmt.Sprintf(row, "nan", "NaN", "0") + fmt.Sprintf(row, "range", "91", "0") + fmt.Sprintf(row, "missing", "", "0") + "ENDSOURCETABLE\n"
	table, err := ParseSourcetable(input)
	if err != nil {
		t.Fatal(err)
	}
	nearest := table.NearestStreams(0, 0, 0)
	if len(nearest) != 2 || nearest[0].Stream.Mountpoint != "first" || nearest[1].Stream.Mountpoint != "second" {
		t.Fatalf("nearest=%+v", nearest)
	}
	if len(table.Warnings) != 4 {
		t.Fatalf("warnings=%+v", table.Warnings)
	}
	if got := table.NearestStreams(math.NaN(), 0, 100); len(got) != 0 {
		t.Fatal("accepted invalid query")
	}
	if got := DistanceMeters(90, 0, -90, 180); !finite(got) || math.Abs(got-earthRadiusMeters*math.Pi) > 1e-6 {
		t.Fatalf("antipodal distance=%v", got)
	}
	if !math.IsNaN(DistanceMeters(91, 0, 0, 0)) {
		t.Fatal("accepted invalid position")
	}
	badBool := strings.Replace(fmt.Sprintf(row, "badBool", "0", "0"), ";0;0;GEN;none;B;N;", ";invalid;0;GEN;none;B;invalid;", 1)
	parsed, err := ParseSourcetable(badBool + "ENDSOURCETABLE\n")
	if err != nil || len(parsed.Warnings) != 2 {
		t.Fatalf("boolean warnings=%+v, error=%v", parsed.Warnings, err)
	}
}

func withChecksum(body string) string { return "$" + body + "*" + checksum(body) }

func TestGGABoundariesAndInvalidFields(t *testing.T) {
	gga := GGA{Time: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), Latitude: 12.999999999, Longitude: -179.999999999, FixQuality: FixGPS, Satellites: 12, HDOP: 1}
	sentence, err := FormatGGA(gga)
	if err != nil || !strings.Contains(sentence, ",1300.0000,N,18000.0000,W,") {
		t.Fatalf("rounded=%q, %v", sentence, err)
	}
	for _, change := range []func(*GGA){
		func(g *GGA) { g.Latitude = math.NaN() }, func(g *GGA) { g.Longitude = math.Inf(1) },
		func(g *GGA) { g.HDOP = math.NaN() }, func(g *GGA) { g.AltitudeMeters = math.Inf(1) },
		func(g *GGA) { g.Satellites = -1 }, func(g *GGA) { g.Satellites = 100 },
		func(g *GGA) { g.FixQuality = 9 }, func(g *GGA) { g.StationID = "1\r\n" }, func(g *GGA) { g.Talker = "G," },
	} {
		invalid := gga
		change(&invalid)
		if _, err := FormatGGA(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("accepted invalid format input: %+v, %v", invalid, err)
		}
	}
	for _, body := range []string{
		"GPGGA,broken\r\nextra-line",
		"GPGGA,123519,4860.000,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,",
		"GPGGA,246000,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,",
		"GPGGA,123519,4807.038,N,01131.000,E,1,08,NaN,545.4,M,46.9,M,,",
		"GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,F,46.9,M,,",
		"GPGGA,123519,9000.00000001,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,",
	} {
		if err := ValidateGGA(withChecksum(body)); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("accepted malformed GGA %q: %v", body, err)
		}
	}
	if err := ValidateGGA(testGGA + "\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGGA(testGGA + "\n"); err == nil {
		t.Fatal("accepted non-CRLF suffix")
	}
	if err := ValidateGGA(withChecksum("GPGGA,,,,,,0,00,,,,,,,")); err != nil {
		t.Fatalf("invalid-fix empty fields: %v", err)
	}
}

func FuzzResponseParsing(f *testing.F) {
	f.Add("HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nRTCM")
	f.Add("ICY 200 OK\r\n\xd3\x00")
	f.Fuzz(func(t *testing.T, wire string) {
		resp, err := readResponse(bufio.NewReader(strings.NewReader(wire)), 64*1024)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
		}
	})
}

func FuzzSourcetableRawRoundTrip(f *testing.F) {
	f.Add("ENDSOURCETABLE\n")
	f.Add("XYZ;unknown\nENDSOURCETABLE\n")
	f.Fuzz(func(t *testing.T, input string) {
		table, err := ParseSourcetable(input)
		if err != nil {
			return
		}
		if _, err := ParseSourcetable(table.RawString()); err != nil {
			t.Fatalf("raw round-trip failed: %v", err)
		}
	})
}

func BenchmarkNearestStreams(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			table := Sourcetable{Streams: make([]StreamEntry, n)}
			for i := range table.Streams {
				table.Streams[i].Latitude = 80 * float64(n-i) / float64(n)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = table.NearestStreams(0, 0, 0)
			}
		})
	}
}

type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

func BenchmarkSourceChunkWrite(b *testing.B) {
	for _, size := range []int{128, 1024, 32768} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			source := &Source{conn: discardConn{}, writeTimeout: time.Second}
			payload := make([]byte, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := source.Write(payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type cancelingInput struct {
	io.Reader
	cancel context.CancelFunc
}

func (r *cancelingInput) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.cancel()
	return n, err
}
func (*cancelingInput) Close() error { return nil }

func TestCanceledSourcePreservesConsumedBlock(t *testing.T) {
	url := serveOnce(t, func(conn net.Conn) {
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
		_, _ = io.Copy(io.Discard, conn)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: url}})
	err := stream.Run(ctx, &cancelingInput{Reader: strings.NewReader("RTCM"), cancel: cancel})
	var delivery *DeliveryError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &delivery) || string(delivery.Data) != "RTCM" || delivery.Written != 0 {
		t.Fatalf("lost cancellation/delivery information: %v", err)
	}
}

func TestPumpCancelsBlockedRead(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &shortSink{}
	done := make(chan error, 1)
	go func() { _, err := PumpRTCM(ctx, sink, reader, nil); done <- err }()
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if !sink.closed {
		t.Fatal("destination not closed")
	}
}

func TestManagedStreamsRejectConcurrentRuns(t *testing.T) {
	for _, source := range []bool{false, true} {
		t.Run(fmt.Sprint(source), func(t *testing.T) {
			connected := make(chan struct{})
			url := serveOnce(t, func(conn net.Conn) {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
				close(connected)
				_, _ = io.Copy(io.Discard, conn)
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			var second error
			if source {
				stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: url}})
				reader, writer := io.Pipe()
				defer writer.Close()
				go func() { done <- stream.Run(ctx, reader) }()
				receive(t, connected)
				second = stream.Run(context.Background(), io.NopCloser(strings.NewReader("unused")))
			} else {
				stream := NewRoverStream(StreamConfig{Rover: RoverConfig{URL: url}})
				handler := func(context.Context, []byte) error { return nil }
				go func() { done <- stream.Run(ctx, handler) }()
				receive(t, connected)
				second = stream.Run(context.Background(), handler)
			}
			if !errors.Is(second, ErrInvalidConfig) {
				t.Fatalf("second run=%v", second)
			}
			cancel()
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("first run=%v", err)
			}
		})
	}
}

func TestSourcetableBoundsParsedObjectAmplification(t *testing.T) {
	if _, err := ParseSourcetable(strings.Repeat("STR\n", 100) + "ENDSOURCETABLE\n"); !errors.Is(err, ErrBadResponse) {
		t.Fatalf("unbounded warnings: %v", err)
	}
	row := "STR;BASE;Base;RTCM 3.3;1005;2;GPS;NET;BEL;0;0;0;0;GEN;none;B;N;9600;misc\n"
	if _, err := ParseSourcetable(strings.Repeat(row, maxSourcetableRecords+1) + "ENDSOURCETABLE\n"); !errors.Is(err, ErrBadResponse) {
		t.Fatalf("unbounded records: %v", err)
	}
}

type readFunc func([]byte) (int, error)

func (r readFunc) Read(p []byte) (int, error) { return r(p) }

func TestSourceRetriesSetupButNotInputErrors(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	var attempts atomic.Int32
	result := make(chan string, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for i := 0; i < 3; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			req, err := http.ReadRequest(bufio.NewReader(conn))
			if err != nil {
				_ = conn.Close()
				return
			}
			attempts.Add(1)
			if i < 2 {
				_, _ = io.WriteString(conn, "HTTP/1.1 503 Unavailable\r\n\r\n")
			} else {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\n\r\n")
				payload := make([]byte, 4)
				if _, err := io.ReadFull(req.Body, payload); err != nil {
					result <- err.Error()
				} else {
					result <- string(payload)
				}
			}
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream := NewSourceStream(SourceStreamConfig{Source: SourceConfig{URL: "http://" + ln.Addr().String() + "/MOUNT"},
		Reconnect: BackoffConfig{Min: time.Millisecond, Max: 2 * time.Millisecond}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	reader := strings.NewReader("RTCM")
	input := io.NopCloser(readFunc(func(p []byte) (int, error) {
		if attempts.Load() != 3 {
			return 0, errors.New("input consumed before successful setup")
		}
		n, _ := reader.Read(p)
		return n, io.ErrUnexpectedEOF // Data and a terminal local error in one read.
	}))
	err := stream.Run(ctx, input)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("input error=%v", err)
	}
	if got := receive(t, result); got != "RTCM" {
		t.Fatalf("payload=%q", got)
	}
	receive(t, serverDone)
	if stats := stream.Stats(); stats.Connections != 1 || stats.BytesWritten != 4 || !errors.Is(stats.LastError, io.ErrUnexpectedEOF) {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestPumpGGARepeatsAndCancels(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	rover := &Rover{conn: client, closeConn: closeWithContext(context.Background(), client), writeTimeout: time.Second}
	sentences := make(chan string, 1)
	sentences <- testGGA
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- PumpGGA(ctx, rover, time.Millisecond, sentences) }()
	reader := bufio.NewReader(server)
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 2; i++ {
		line, err := reader.ReadString('\n')
		if err != nil || line != testGGA+"\r\n" {
			t.Fatalf("GGA=%q, error=%v", line, err)
		}
	}
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("pump=%v", err)
	}
	if err := rover.WriteGGA(testGGA); err == nil {
		t.Fatal("pump did not close its rover")
	}
}

func TestSourceFinishReportsCloseError(t *testing.T) {
	failure := errors.New("close failed")
	conn := &faultConn{closeErr: failure}
	source := &Source{conn: conn, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
	if err := source.Finish(); !errors.Is(err, failure) {
		t.Fatalf("finish=%v", err)
	}
	if conn.output.String() != "0\r\n\r\n" || conn.closes != 1 {
		t.Fatalf("wire=%q, closes=%d", conn.output.String(), conn.closes)
	}
}

func TestStatusClassificationIgnoresErrorBodyEncoding(t *testing.T) {
	resp, err := readResponse(bufio.NewReader(strings.NewReader("HTTP/1.1 503 Unavailable\r\nContent-Encoding: gzip\r\n\r\n")), DefaultMaxHeaderBytes)
	if err != nil {
		t.Fatal(err)
	}
	err = expectOK(resp)
	var status *StatusError
	if !errors.As(err, &status) || status.Code != 503 || !Temporary(err) {
		t.Fatalf("lost HTTP status classification: %v", err)
	}
}

func TestSourceRejectsLegacyAcknowledgment(t *testing.T) {
	for _, acknowledgment := range []string{"ICY 200 OK\r\n", "HTTP/1.0 200 OK\r\n\r\n"} {
		url := serveOnce(t, func(conn net.Conn) {
			if _, err := http.ReadRequest(bufio.NewReader(conn)); err == nil {
				_, _ = io.WriteString(conn, acknowledgment)
			}
		})
		if source, err := DialSource(context.Background(), SourceConfig{URL: url}); !errors.Is(err, ErrBadResponse) {
			if source != nil {
				_ = source.Close()
			}
			t.Fatalf("accepted legacy acknowledgment %q: %v", acknowledgment, err)
		}
	}
}

func TestCancelWhileWaitingForHeaders(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(context.Context, string) error
	}{
		{"rover", func(ctx context.Context, url string) error {
			r, err := DialRover(ctx, RoverConfig{URL: url})
			if r != nil {
				_ = r.Close()
			}
			return err
		}},
		{"source", func(ctx context.Context, url string) error {
			s, err := DialSource(ctx, SourceConfig{URL: url})
			if s != nil {
				_ = s.Close()
			}
			return err
		}},
		{"sourcetable", func(ctx context.Context, url string) error {
			_, err := FetchSourcetable(ctx, SourcetableConfig{URL: url})
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			requested := make(chan struct{})
			url := serveOnce(t, func(conn net.Conn) {
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				close(requested)
				_, _ = io.Copy(io.Discard, conn)
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation.run(ctx, url) }()
			receive(t, requested)
			cancel()
			if err := receive(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel error=%v", err)
			}
		})
	}
}

func TestGGARejectsExtraTerminatorsBeforeNormalization(t *testing.T) {
	for _, sentence := range []string{testGGA + "\r\n\r\n", testGGA + "\n", " " + testGGA, testGGA + " "} {
		conn := &faultConn{}
		rover := &Rover{conn: conn, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
		if err := rover.WriteGGA(sentence); !errors.Is(err, ErrInvalidConfig) || conn.output.Len() != 0 {
			t.Fatalf("wrote invalid sentence %q: %v", sentence, err)
		}
		if err := NewRoverStream(StreamConfig{}).SetGGA(sentence); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("stored invalid sentence %q: %v", sentence, err)
		}
		_ = rover.Close()
	}
	conn := &faultConn{}
	rover := &Rover{conn: conn, closeConn: closeWithContext(context.Background(), conn), writeTimeout: time.Second}
	defer rover.Close()
	if err := rover.WriteGGA(testGGA + "\r\n"); err != nil || conn.output.String() != testGGA+"\r\n" {
		t.Fatalf("valid terminator normalized incorrectly: wire=%q, error=%v", conn.output.String(), err)
	}
}

func FuzzGGAValidation(f *testing.F) {
	f.Add(testGGA)
	f.Add("$GPGGA,broken\r\nextra-line*3B")
	f.Fuzz(func(t *testing.T, sentence string) {
		valid := ValidateGGA(sentence) == nil
		if stored := NewRoverStream(StreamConfig{}).SetGGA(sentence) == nil; stored != valid {
			t.Fatalf("SetGGA disagrees with validation for %q", sentence)
		}
		if valid {
			wire := normalizeSentence(sentence) + "\r\n"
			if len(wire) > 82 || strings.Count(wire, "\n") != 1 || strings.Count(wire, "\r") != 1 {
				t.Fatalf("accepted unsafe NMEA framing: %q", wire)
			}
		}
	})
}
