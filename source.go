package ntrip

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// Source publishes a chunked NTRIP v2 POST body. Writes are serialized. Close
// aborts immediately, including a blocked Write; Finish ends the body gracefully.
type Source struct {
	conn         net.Conn
	mu           sync.Mutex
	header       map[string][]string
	closeConn    func() error
	writeTimeout time.Duration
	writeErr     error
	finished     bool
	finishErr    error
	// Scratch framing is protected by mu. Payload references are cleared after
	// each write so an idle source cannot retain a caller's large backing buffer.
	frameHeader  [18]byte
	frameEnd     [2]byte
	frameBuffers [3][]byte
	frames       net.Buffers
}

func DialSource(ctx context.Context, cfg SourceConfig) (*Source, error) {
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return nil, err
	}
	request, err := requestBytes("POST", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers,
		map[string]string{"Content-Type": "gnss/data", "Transfer-Encoding": "chunked"})
	if err != nil {
		return nil, err
	}
	conn, err := dial(ctx, ep, dialTimeout(cfg.DialTimeout), cfg.Dialer, cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	closeConn := closeWithContext(ctx, conn)
	if _, err := writeFull(conn, request, writeTimeout(cfg.WriteTimeout)); err != nil {
		_ = closeConn()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("write ntrip source request: %w", err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	var resp response
	err = withReadDeadline(conn, headerTimeout(cfg.HeaderTimeout), func() error {
		var err error
		resp, err = readResponse(reader, maxHeaderBytes(cfg.MaxHeaderBytes))
		return err
	})
	if err == nil {
		err = expectOK(resp)
	}
	if err == nil && resp.Proto != "HTTP/1.1" {
		err = fmt.Errorf("%w: chunked source upload requires an HTTP/1.1 acknowledgment", ErrBadResponse)
	}
	if err != nil {
		_ = closeConn()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if ctx.Err() != nil {
		_ = closeConn()
		return nil, ctx.Err()
	}
	return &Source{conn: conn, header: resp.Header, closeConn: closeConn, writeTimeout: writeTimeout(cfg.WriteTimeout)}, nil
}

func (s *Source) Header() map[string][]string {
	out := make(map[string][]string, len(s.header))
	for key, values := range s.header {
		out[key] = append([]string(nil), values...)
	}
	return out
}

// Write returns payload bytes accepted by the connection, excluding framing.
// These counts are not delivery acknowledgments. Any failure poisons the stream:
// further chunks cannot repair a partially written chunk and are rejected.
func (s *Source) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return 0, net.ErrClosed
	}
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		s.writeErr = err
		_ = s.Close()
		return 0, err
	}
	defer s.conn.SetWriteDeadline(time.Time{})
	// 16 hex digits and CRLF cover any int-sized payload.
	header := strconv.AppendInt(s.frameHeader[:0], int64(len(p)), 16)
	header = append(header, '\r', '\n')
	s.frameEnd = [2]byte{'\r', '\n'}
	s.frameBuffers = [3][]byte{header, p, s.frameEnd[:]}
	s.frames = s.frameBuffers[:]
	defer func() { clear(s.frameBuffers[:]); s.frames = nil }()
	n, err := s.frames.WriteTo(s.conn)
	if err == nil && n != int64(len(header))+int64(len(p))+2 {
		err = io.ErrShortWrite
	}
	if err != nil {
		s.writeErr = err
		_ = s.Close()
	}
	return int(max(0, min(int64(len(p)), n-int64(len(header))))), err
}

// Finish writes the final chunk and closes the connection. It reports framing
// and close errors, but cannot confirm that the caster processed the data.
func (s *Source) Finish() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.finished {
		s.finished = true
		s.finishErr = s.writeErr
		if s.finishErr == nil {
			_, s.finishErr = writeFull(s.conn, []byte("0\r\n\r\n"), s.writeTimeout)
		}
		s.finishErr = errors.Join(s.finishErr, s.Close())
	}
	return s.finishErr
}

func (s *Source) Close() error {
	return s.closeConn()
}
