package ntrip

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type response struct {
	Proto  string
	Code   int
	Status string
	Header map[string][]string
	Body   io.ReadCloser
}

func dial(ctx context.Context, ep endpoint, timeout time.Duration, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
	setupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if dialer == nil {
		dialer = &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	} else if dialer.Timeout == 0 {
		copy := *dialer
		copy.Timeout = timeout
		dialer = &copy
	}
	conn, err := dialer.DialContext(setupCtx, ep.Network, ep.Address)
	if err != nil {
		if setupCtx.Err() != nil && ctx.Err() == nil {
			err = os.ErrDeadlineExceeded
		}
		return nil, fmt.Errorf("dial ntrip caster: %w", err)
	}
	if !ep.TLS {
		return conn, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.URL.Hostname()}
	if tlsConfig != nil {
		cfg = tlsConfig.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = ep.URL.Hostname()
		}
		if cfg.MinVersion == 0 {
			cfg.MinVersion = tls.VersionTLS12
		}
	}
	// This transport speaks HTTP/1.1 even when the supplied TLS config offers h2.
	cfg.NextProtos = []string{"http/1.1"}
	tlsConn := tls.Client(conn, cfg)
	if err := tlsConn.HandshakeContext(setupCtx); err != nil {
		_ = conn.Close()
		if setupCtx.Err() != nil && ctx.Err() == nil {
			err = os.ErrDeadlineExceeded
		}
		return nil, fmt.Errorf("handshake ntrip caster tls: %w", err)
	}
	return tlsConn, nil
}

// closeWithContext takes ownership of closer. The returned finalizer closes it
// exactly once and joins an in-progress cancellation callback before returning.
func closeWithContext(ctx context.Context, closer io.Closer) func() error {
	var err error
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		err = closer.Close()
		close(done)
	})
	var once sync.Once
	return func() error {
		once.Do(func() {
			if stop() {
				err = closer.Close()
			} else {
				<-done
			}
		})
		return err
	}
}

func writeFull(conn net.Conn, p []byte, timeout time.Duration) (int, error) {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, err
		}
		defer conn.SetWriteDeadline(time.Time{})
	}
	total := 0
	for len(p) > 0 {
		n, err := conn.Write(p)
		total += n
		if err != nil {
			return total, err
		}
		p = p[n:]
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func withReadDeadline(conn net.Conn, timeout time.Duration, fn func() error) error {
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer conn.SetReadDeadline(time.Time{})
	}
	return fn()
}

func readResponse(reader *bufio.Reader, limit uint64) (response, error) {
	lr := &limitedLineReader{reader: reader, remaining: limit}
	for {
		statusLine, err := lr.readLine()
		if err != nil {
			return response{}, fmt.Errorf("%w: read status line: %w", ErrBadResponse, err)
		}
		resp, err := parseStatusLine(statusLine)
		if err != nil {
			return response{}, err
		}
		if resp.Proto == "ICY" {
			// Legacy ICY success consists of one status line followed immediately by
			// binary data. Do not consume binary bytes looking for HTTP headers.
			resp.Header = make(map[string][]string)
			resp.Body = io.NopCloser(reader)
			return resp, nil
		}
		var headers strings.Builder
		headers.WriteString(statusLine + "\r\n")
		for {
			line, err := lr.readLine()
			if err != nil {
				return response{}, fmt.Errorf("%w: read header: %w", ErrBadResponse, err)
			}
			headers.WriteString(line + "\r\n")
			if line == "" {
				break
			}
		}
		// Bound the header block first, then let net/http validate and decode HTTP
		// framing (including exact Content-Length and chunk trailers). Its buffered
		// reader owns any body bytes read ahead, so always read through parsed.Body.
		var raw io.Reader = strings.NewReader(headers.String())
		if resp.Code >= 200 {
			raw = io.MultiReader(raw, reader)
		}
		// Informational responses must not read ahead into the next response:
		// their temporary parser is discarded before reading the next header block.
		parsed, err := http.ReadResponse(bufio.NewReader(raw), nil)
		if err != nil {
			return response{}, fmt.Errorf("%w: %w", ErrBadResponse, err)
		}
		if parsed.StatusCode >= 100 && parsed.StatusCode < 200 {
			if parsed.StatusCode == http.StatusSwitchingProtocols {
				return response{}, fmt.Errorf("%w: protocol switching is unsupported", ErrBadResponse)
			}
			continue
		}
		if parsed.StatusCode == http.StatusOK {
			for _, encoding := range parsed.Header.Values("Content-Encoding") {
				if !strings.EqualFold(encoding, "identity") {
					return response{}, fmt.Errorf("%w: unsupported content encoding %q", ErrBadResponse, encoding)
				}
			}
		}
		resp.Header = parsed.Header
		resp.Body = parsed.Body
		return resp, nil
	}
}

func parseStatusLine(line string) (response, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 || (parts[0] != "HTTP/1.1" && parts[0] != "HTTP/1.0" && parts[0] != "ICY") || len(parts[1]) != 3 {
		return response{}, fmt.Errorf("%w: malformed status line %q", ErrBadResponse, line)
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil || code < 100 || code > 599 {
		return response{}, fmt.Errorf("%w: malformed status line %q", ErrBadResponse, line)
	}
	return response{Proto: parts[0], Code: code, Status: line}, nil
}

type limitedLineReader struct {
	reader    *bufio.Reader
	remaining uint64
}

func (r *limitedLineReader) readLine() (string, error) {
	var line []byte
	for {
		fragment, err := r.reader.ReadSlice('\n')
		if uint64(len(fragment)) > r.remaining {
			return "", fmt.Errorf("response headers exceed configured limit")
		}
		r.remaining -= uint64(len(fragment))
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return "", err
		}
		if len(line) < 2 || line[len(line)-2] != '\r' {
			return "", fmt.Errorf("response lines must end with CRLF")
		}
		return string(line[:len(line)-2]), nil
	}
}
