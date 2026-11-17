package ntrip

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Rover supports one active reader alongside serialized GGA/raw writes. Close is
// safe concurrently with I/O and aborts the underlying connection immediately.
type Rover struct {
	conn   net.Conn
	reader io.Reader
	// Serializes writes so caller-driven GGA updates and timed GGA updates never
	// interleave bytes on the same TCP/TLS connection.
	mu           sync.Mutex
	header       map[string][]string
	closeConn    func() error
	writeTimeout time.Duration
}

func DialRover(ctx context.Context, cfg RoverConfig) (*Rover, error) {
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return nil, err
	}
	request, err := requestBytes("GET", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers, nil)
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
		_ = closeConn()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if err := expectOK(resp); err != nil {
		_ = closeConn()
		return nil, err
	}
	if ctx.Err() != nil {
		_ = closeConn()
		return nil, ctx.Err()
	}
	return &Rover{conn: conn, reader: resp.Body, header: resp.Header, closeConn: closeConn, writeTimeout: writeTimeout(cfg.WriteTimeout)}, nil
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
	if err := ValidateGGA(sentence); err != nil {
		return err
	}
	sentence = normalizeSentence(sentence)
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := writeFull(r.conn, []byte(sentence+"\r\n"), r.writeTimeout)
	if err != nil {
		_ = r.Close()
	}
	return err
}

func (r *Rover) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, err := writeFull(r.conn, p, r.writeTimeout)
	if err != nil {
		_ = r.Close()
	}
	return n, err
}

func (r *Rover) Close() error {
	return r.closeConn()
}
