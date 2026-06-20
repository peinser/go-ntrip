package ntrip

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

type Rover struct {
	conn   net.Conn
	reader *bufio.Reader
	// Serializes writes so caller-driven GGA updates and timed GGA updates never
	// interleave bytes on the same TCP/TLS connection.
	mu               sync.Mutex
	header           map[string][]string
	stopContextClose func() bool
	writeTimeout     time.Duration
}

func DialRover(ctx context.Context, cfg RoverConfig) (*Rover, error) {
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return nil, err
	}
	conn, err := dial(ctx, ep, dialTimeout(cfg.DialTimeout), cfg.Dialer, cfg.TLSConfig)
	if err != nil {
		return nil, err
	}
	stopContextClose := closeOnContext(ctx, conn)
	if err := writeRequest(conn, writeTimeout(cfg.WriteTimeout), "GET", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers, nil); err != nil {
		stopContextClose()
		_ = conn.Close()
		return nil, fmt.Errorf("write ntrip rover request: %w", err)
	}
	reader := bufio.NewReaderSize(conn, 32*1024)
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
	return &Rover{conn: conn, reader: reader, header: resp.Header, stopContextClose: stopContextClose, writeTimeout: writeTimeout(cfg.WriteTimeout)}, nil
}

func (r *Rover) Header() map[string][]string {
	out := make(map[string][]string, len(r.header))
	for key, values := range r.header {
		out[key] = append([]string(nil), values...)
	}
	return out
}

func (r *Rover) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *Rover) WriteGGA(sentence string) error {
	sentence = normalizeSentence(sentence)
	if err := ValidateGGA(sentence); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return writeFull(r.conn, []byte(sentence+"\r\n"), r.writeTimeout)
}

func (r *Rover) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := writeFull(r.conn, p, r.writeTimeout); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (r *Rover) Close() error {
	if r.stopContextClose != nil {
		r.stopContextClose()
	}
	return r.conn.Close()
}
