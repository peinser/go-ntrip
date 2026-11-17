package ntrip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// RTCMHandler is called synchronously with arbitrary stream fragments, not whole
// RTCM messages. p is borrowed until the callback returns; copy it to retain it.
// A blocking handler must honor ctx. Any handler error stops Run without retry.
type RTCMHandler func(ctx context.Context, p []byte) error

type StreamStats struct {
	Connections  uint64
	Reconnects   uint64
	BytesRead    uint64
	BytesWritten uint64
	LastError    error
}

type RoverStream struct {
	cfg     StreamConfig
	mu      sync.RWMutex
	latest  string
	stats   StreamStats
	running bool
}

type SourceStream struct {
	cfg     SourceStreamConfig
	mu      sync.RWMutex
	stats   StreamStats
	running bool
}

func NewRoverStream(cfg StreamConfig) *RoverStream         { return &RoverStream{cfg: cfg} }
func NewSourceStream(cfg SourceStreamConfig) *SourceStream { return &SourceStream{cfg: cfg} }

// SetGGA stores a complete, checksummed sentence. The caller must refresh its
// timestamp/position as needed; the stream never rewrites supplied NMEA data.
func (s *RoverStream) SetGGA(sentence string) error {
	if err := ValidateGGA(sentence); err != nil {
		return err
	}
	sentence = normalizeSentence(sentence)
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

// RunToWriter owns dst and closes it on return or cancellation. Close must
// unblock Write. If a session fails during a blocked write, dst is closed and
// the run stops rather than reconnecting with an unusable destination.
func (s *RoverStream) RunToWriter(ctx context.Context, dst io.WriteCloser) (err error) {
	if dst == nil {
		return fmt.Errorf("%w: destination is required", ErrInvalidConfig)
	}
	closeDst := closeWithContext(ctx, dst)
	defer func() {
		err = errors.Join(err, closeDst())
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	return s.Run(ctx, func(sessionCtx context.Context, p []byte) error {
		done := make(chan struct{})
		stop := context.AfterFunc(sessionCtx, func() { _ = closeDst(); close(done) })
		n, err := dst.Write(p)
		if !stop() {
			<-done
			// Close may race a successful Write. Once cancellation closed dst,
			// it cannot be reused on another session even if Write returned nil.
			if err == nil {
				err = context.Cause(sessionCtx)
			}
		}
		if err == nil && n != len(p) {
			err = io.ErrShortWrite
		}
		return err
	})
}

// Run permits one active invocation per stream. Stats and SetGGA are safe to
// call concurrently. No reader, GGA writer, or callback outlives Run.
func (s *RoverStream) Run(ctx context.Context, handler RTCMHandler) (err error) {
	if handler == nil {
		return fmt.Errorf("%w: rtcm handler is required", ErrInvalidConfig)
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("%w: rover stream is already running", ErrInvalidConfig)
	}
	s.running = true
	s.mu.Unlock()
	defer func() {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		s.mu.Lock()
		s.running = false
		s.stats.LastError = err
		s.mu.Unlock()
	}()
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
		retry := true
		if err == nil {
			s.recordConnect()
			started := time.Now()
			before := s.Stats().BytesRead
			if latest := s.latestGGA(); latest != "" {
				err = rover.WriteGGA(latest)
				if err == nil {
					s.addBytesWritten(uint64(len(latest) + 2))
				}
			}
			if err == nil {
				err, retry = s.runConnected(ctx, rover, buf, handler)
			}
			_ = rover.Close()
			if time.Since(started) >= backoff.ResetAfter && s.Stats().BytesRead > before {
				delay = backoff.Min
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		s.recordError(err)
		if !retry || (errors.Is(err, io.EOF) && !s.cfg.ReconnectOnEOF) || !Temporary(err) {
			return err
		}
		logger.Warn("ntrip rover stream reconnecting", "error", err, "backoff", delay)
		if err := waitBackoff(ctx, delay); err != nil {
			return err
		}
		delay = nextDelay(delay, backoff)
	}
}

func (s *RoverStream) runConnected(ctx context.Context, rover *Rover, buf []byte, handler RTCMHandler) (error, bool) {
	sessionCtx, cancel := context.WithCancelCause(ctx)
	interval := s.cfg.GGAInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				_ = rover.Close()
				return
			case <-ticker.C:
				latest := s.latestGGA()
				if latest == "" {
					continue
				}
				if err := rover.WriteGGA(latest); err != nil {
					cancel(err)
					_ = rover.Close()
					return
				}
				s.addBytesWritten(uint64(len(latest) + 2))
			}
		}
	}()
	defer func() {
		cancel(nil)
		_ = rover.Close() // Also interrupt a GGA write when a local handler stops.
		<-done
	}()
	for {
		if cause := context.Cause(sessionCtx); cause != nil {
			return cause, true
		}
		if err := rover.conn.SetReadDeadline(time.Now().Add(readIdleTimeout(s.cfg.ReadIdleTimeout))); err != nil {
			return err, true
		}
		n, err := rover.Read(buf)
		if cause := context.Cause(sessionCtx); cause != nil {
			return cause, true
		}
		if n > 0 {
			s.addBytesRead(uint64(n))
			if hErr := handler(sessionCtx, buf[:n]); hErr != nil {
				return hErr, false
			}
		}
		if err != nil {
			return err, true
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
	// Equal jitter: sample the upper half of the nominal delay, staying <= Max.
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay-delay/2)+1))
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
	if float64(current)*cfg.Factor >= float64(cfg.Max) {
		return cfg.Max
	}
	return max(current, time.Duration(float64(current)*cfg.Factor))
}

func (s *SourceStream) Stats() StreamStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stats
}

// Run owns src, whose Close must unblock Read. Connection setup retries transient
// errors. Once input is consumed, read errors and uncertain writes stop the run;
// no bytes are silently skipped or replayed on a new connection.
// A rejected concurrent invocation does not take ownership of its input.
func (s *SourceStream) Run(ctx context.Context, src io.ReadCloser) (err error) {
	if src == nil {
		return fmt.Errorf("%w: rtcm source is required", ErrInvalidConfig)
	}
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("%w: source stream is already running", ErrInvalidConfig)
	}
	s.running = true
	s.mu.Unlock()
	closeInput := closeWithContext(ctx, src)
	defer func() {
		err = errors.Join(err, closeInput())
		if ctx.Err() != nil {
			var deliveryErr *DeliveryError
			if errors.As(err, &deliveryErr) {
				err = errors.Join(ctx.Err(), err)
			} else {
				err = ctx.Err()
			}
		}
		s.mu.Lock()
		s.running = false
		s.stats.LastError = err
		s.mu.Unlock()
	}()
	logger := s.cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	backoff := s.cfg.Reconnect.normalized()
	delay := backoff.Min
	var source *Source
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		source, err = DialSource(ctx, s.cfg.Source)
		if err == nil {
			break
		}
		s.recordError(err)
		if !Temporary(err) {
			return err
		}
		logger.Warn("ntrip source stream connecting", "error", err, "backoff", delay)
		if err := waitBackoff(ctx, delay); err != nil {
			return err
		}
		delay = nextDelay(delay, backoff)
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	s.recordConnect()
	buf := make([]byte, bufferSize(s.cfg.BufferSize))
	emptyReads := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, readErr := src.Read(buf)
		if ctx.Err() != nil {
			if n > 0 {
				return &DeliveryError{Data: append([]byte(nil), buf[:n]...), Err: ctx.Err()}
			}
			return ctx.Err()
		}
		if n > 0 {
			emptyReads = 0
			written, writeErr := source.Write(buf[:n])
			s.addBytesWritten(uint64(written))
			if writeErr != nil {
				if readErr != nil && !errors.Is(readErr, io.EOF) {
					writeErr = errors.Join(writeErr, readErr)
				}
				return &DeliveryError{Data: append([]byte(nil), buf[:n]...), Written: written, Err: writeErr}
			}
		} else if readErr == nil {
			emptyReads++
			if emptyReads >= 100 {
				return io.ErrNoProgress
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return source.Finish()
			}
			return readErr
		}
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
