package ntrip

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type Source struct {
	conn    net.Conn
	chunked bool
	// Serializes chunk framing. Concurrent chunk writers would corrupt the stream.
	mu               sync.Mutex
	header           map[string][]string
	stopContextClose func() bool
	writeTimeout     time.Duration
}

func DialSource(ctx context.Context, cfg SourceConfig) (*Source, error) {
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return nil, err
	}
	conn, err := dial(ctx, ep, dialTimeout(cfg.DialTimeout), cfg.Dialer, cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	stopContextClose := closeOnContext(ctx, conn)
	extra := map[string]string{"Content-Type": "application/octet-stream"}
	if !cfg.NoChunked {
		extra["Transfer-Encoding"] = "chunked"
	}
	if err := writeRequest(conn, writeTimeout(cfg.WriteTimeout), "PUT", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers, extra); err != nil {
		stopContextClose()
		_ = conn.Close()
		return nil, fmt.Errorf("write ntrip source request: %w", err)
	}
	reader := bufio.NewReaderSize(conn, 4096)
	var resp response
	err = withReadDeadline(conn, headerTimeout(cfg.HeaderTimeout), func() error {
		var err error
		resp, err = readResponse(reader, maxHeaderBytes(cfg.MaxHeaderBytes))
		return err
	})
	if err != nil {
		stopContextClose()
		_ = conn.Close()
		return nil, err
	}
	if err := expectOK(resp); err != nil {
		stopContextClose()
		_ = conn.Close()
		return nil, err
	}
	return &Source{conn: conn, chunked: !cfg.NoChunked, header: resp.Header, stopContextClose: stopContextClose, writeTimeout: writeTimeout(cfg.WriteTimeout)}, nil
}

func (s *Source) Header() map[string][]string {
	out := make(map[string][]string, len(s.header))
	for key, values := range s.header {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func (s *Source) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.chunked {
		if err := writeFull(s.conn, p, s.writeTimeout); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if len(p) == 0 {
		return 0, nil
	}
	frame := []byte(fmt.Sprintf("%x\r\n", len(p)))
	frame = append(frame, p...)
	frame = append(frame, '\r', '\n')
	if err := writeFull(s.conn, frame, s.writeTimeout); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (s *Source) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.chunked {
		_ = writeFull(s.conn, []byte("0\r\n\r\n"), s.writeTimeout)
	}
	if s.stopContextClose != nil {
		s.stopContextClose()
	}
	return s.conn.Close()
}
