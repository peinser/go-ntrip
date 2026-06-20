package ntrip

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type response struct {
	Proto  string
	Code   int
	Status string
	Header map[string][]string
}

func dial(ctx context.Context, ep endpoint, timeout time.Duration, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
	if dialer == nil {
		dialer = &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	} else if dialer.Timeout == 0 {
		copy := *dialer
		copy.Timeout = timeout
		dialer = &copy
	}
	conn, err := dialer.DialContext(ctx, ep.Network, ep.Address)
	if err != nil {
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
	tlsConn := tls.Client(conn, cfg)
	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("handshake ntrip caster tls: %w", err)
	}
	_ = tlsConn.SetDeadline(time.Time{})
	return tlsConn, nil
}

func closeOnContext(ctx context.Context, conn net.Conn) func() bool {
	return context.AfterFunc(ctx, func() { _ = conn.Close() })
}

func writeFull(conn net.Conn, p []byte, timeout time.Duration) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer conn.SetWriteDeadline(time.Time{})
	}
	for len(p) > 0 {
		n, err := conn.Write(p)
		if err != nil {
			return err
		}
		p = p[n:]
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
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
	statusLine, err := lr.readLine()
	if err != nil {
		return response{}, fmt.Errorf("%w: read status line: %v", ErrBadResponse, err)
	}
	resp, err := parseStatusLine(statusLine)
	if err != nil {
		return response{}, err
	}
	resp.Header = make(map[string][]string)
	for {
		line, err := lr.readLine()
		if err != nil {
			return response{}, fmt.Errorf("%w: read header: %v", ErrBadResponse, err)
		}
		if line == "" {
			return resp, nil
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return response{}, fmt.Errorf("%w: malformed header %q", ErrBadResponse, line)
		}
		key := canonicalHeaderKey(strings.TrimSpace(name))
		resp.Header[key] = append(resp.Header[key], strings.TrimSpace(value))
	}
}

func parseStatusLine(line string) (response, error) {
	if strings.HasPrefix(line, "ICY ") {
		parts := strings.SplitN(line, " ", 3)
		if len(parts) < 2 {
			return response{}, fmt.Errorf("%w: malformed icy status %q", ErrBadResponse, line)
		}
		code, err := strconv.Atoi(parts[1])
		if err != nil {
			return response{}, fmt.Errorf("%w: malformed icy status %q", ErrBadResponse, line)
		}
		return response{Proto: "ICY", Code: code, Status: line}, nil
	}
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "HTTP/") {
		return response{}, fmt.Errorf("%w: malformed status line %q", ErrBadResponse, line)
	}
	code, err := strconv.Atoi(parts[1])
	if err != nil {
		return response{}, fmt.Errorf("%w: malformed status line %q", ErrBadResponse, line)
	}
	return response{Proto: parts[0], Code: code, Status: line}, nil
}

func canonicalHeaderKey(key string) string {
	parts := strings.Split(strings.ToLower(key), "-")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, "-")
}

type limitedLineReader struct {
	reader    *bufio.Reader
	remaining uint64
}

func (r *limitedLineReader) readLine() (string, error) {
	line, err := r.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if uint64(len(line)) > r.remaining {
		return "", fmt.Errorf("headers exceed %d bytes", r.remaining)
	}
	r.remaining -= uint64(len(line))
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}
