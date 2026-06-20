package ntrip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

type RTCMHandler func([]byte) error

type StreamStats struct {
	Connections  uint64
	Reconnects   uint64
	BytesRead    uint64
	BytesWritten uint64
	LastError    error
}

type RoverStream struct {
	cfg StreamConfig

	// Protects latest GGA and stats. Network read/write synchronization happens on
	// Rover itself, because net.Conn supports concurrent reads and writes.
	mu     sync.RWMutex
	latest string
	stats  StreamStats
}

type SourceStream struct {
	cfg SourceStreamConfig

	// Protects stats only; source writes are serialized by Source.
	mu    sync.RWMutex
	stats StreamStats
}

func NewRoverStream(cfg StreamConfig) *RoverStream {
	return &RoverStream{cfg: cfg}
}

func NewSourceStream(cfg SourceStreamConfig) *SourceStream {
	return &SourceStream{cfg: cfg}
}

func (s *RoverStream) SetGGA(sentence string) error {
	sentence = normalizeSentence(sentence)
	if err := ValidateGGA(sentence); err != nil {
		return err
	}
	s.mu.Lock()
	s.latest = sentence
	s.mu.Unlock()
	return nil
}

func (s *RoverStream) Stats() StreamStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *RoverStream) RunToWriter(ctx context.Context, dst io.Writer) error {
	return s.Run(ctx, func(p []byte) error {
		_, err := dst.Write(p)
		return err
	})
}

func (s *RoverStream) Run(ctx context.Context, handler RTCMHandler) error {
	if handler == nil {
		return fmt.Errorf("%w: rtcm handler is required", ErrInvalidConfig)
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	backoff := s.cfg.Reconnect.normalized()
	delay := backoff.Min
	buf := make([]byte, bufferSize(s.cfg.BufferSize))
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rover, err := DialRover(ctx, s.cfg.Rover)
		if err != nil {
			s.recordError(err)
			if !Temporary(err) {
				return err
			}
			if waitErr := waitBackoff(ctx, delay); waitErr != nil {
				return waitErr
			}
			delay = nextDelay(delay, backoff)
			continue
		}
		s.recordConnect()
		delay = backoff.Min
		if latest := s.latestGGA(); latest != "" {
			if err := rover.WriteGGA(latest); err != nil {
				_ = rover.Close()
				s.recordError(err)
				if !Temporary(err) {
					return err
				}
				continue
			}
			s.addBytesWritten(uint64(len(latest) + 2))
		}
		err = s.runConnected(ctx, rover, buf, handler)
		_ = rover.Close()
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, io.EOF) && !s.cfg.ReconnectOnEOF {
			return err
		}
		s.recordError(err)
		if !Temporary(err) {
			return err
		}
		logger.Warn("ntrip rover stream reconnecting", "error", err, "delay", delay)
		if waitErr := waitBackoff(ctx, delay); waitErr != nil {
			return waitErr
		}
		delay = nextDelay(delay, backoff)
	}
}

func (s *RoverStream) runConnected(ctx context.Context, rover *Rover, buf []byte, handler RTCMHandler) error {
	interval := s.cfg.GGAInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if timeout := readIdleTimeout(s.cfg.ReadIdleTimeout); timeout > 0 {
				_ = rover.conn.SetReadDeadline(time.Now().Add(timeout))
			}
			n, err := rover.Read(buf)
			_ = rover.conn.SetReadDeadline(time.Time{})
			if n > 0 {
				chunk := buf[:n]
				if hErr := handler(chunk); hErr != nil {
					errCh <- hErr
					return
				}
				s.addBytesRead(uint64(n))
			}
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					errCh <- fmt.Errorf("ntrip rover stream idle for %s: %w", readIdleTimeout(s.cfg.ReadIdleTimeout), err)
					return
				}
				errCh <- err
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			_ = rover.Close()
			<-done
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-ticker.C:
			latest := s.latestGGA()
			if latest == "" {
				continue
			}
			if err := rover.WriteGGA(latest); err != nil {
				return err
			}
			s.addBytesWritten(uint64(len(latest) + 2))
		}
	}
}

func (s *RoverStream) latestGGA() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latest
}

func (s *RoverStream) recordConnect() {
	s.mu.Lock()
	s.stats.Connections++
	if s.stats.Connections > 1 {
		s.stats.Reconnects++
	}
	s.stats.LastError = nil
	s.mu.Unlock()
}

func (s *RoverStream) recordError(err error) {
	s.mu.Lock()
	s.stats.LastError = err
	s.mu.Unlock()
}

func (s *RoverStream) addBytesRead(n uint64) {
	s.mu.Lock()
	s.stats.BytesRead += n
	s.mu.Unlock()
}

func (s *RoverStream) addBytesWritten(n uint64) {
	s.mu.Lock()
	s.stats.BytesWritten += n
	s.mu.Unlock()
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextDelay(current time.Duration, cfg BackoffConfig) time.Duration {
	next := time.Duration(float64(current) * cfg.Factor)
	if next > cfg.Max {
		return cfg.Max
	}
	if next <= current {
		return cfg.Max
	}
	return next
}

func (s *SourceStream) Stats() StreamStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

func (s *SourceStream) Run(ctx context.Context, src io.Reader) error {
	if src == nil {
		return fmt.Errorf("%w: rtcm source is required", ErrInvalidConfig)
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	backoff := s.cfg.Reconnect.normalized()
	delay := backoff.Min
	buf := make([]byte, bufferSize(s.cfg.BufferSize))
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		source, err := DialSource(ctx, s.cfg.Source)
		if err != nil {
			s.recordError(err)
			if !Temporary(err) {
				return err
			}
			if waitErr := waitBackoff(ctx, delay); waitErr != nil {
				return waitErr
			}
			delay = nextDelay(delay, backoff)
			continue
		}
		s.recordConnect()
		delay = backoff.Min
		n, err := io.CopyBuffer(source, src, buf)
		s.addBytesWritten(uint64(n))
		_ = source.Close()
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		s.recordError(err)
		if !Temporary(err) {
			return err
		}
		logger.Warn("ntrip source stream reconnecting", "error", err, "delay", delay)
		if waitErr := waitBackoff(ctx, delay); waitErr != nil {
			return waitErr
		}
		delay = nextDelay(delay, backoff)
	}
}

func (s *SourceStream) recordConnect() {
	s.mu.Lock()
	s.stats.Connections++
	if s.stats.Connections > 1 {
		s.stats.Reconnects++
	}
	s.stats.LastError = nil
	s.mu.Unlock()
}

func (s *SourceStream) recordError(err error) {
	s.mu.Lock()
	s.stats.LastError = err
	s.mu.Unlock()
}

func (s *SourceStream) addBytesWritten(n uint64) {
	s.mu.Lock()
	s.stats.BytesWritten += n
	s.mu.Unlock()
}
