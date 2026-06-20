package ntrip

import (
	"context"
	"io"
	"time"
)

func PumpRTCM(ctx context.Context, dst io.Writer, src io.Reader, buf []byte) (int64, error) {
	if len(buf) == 0 {
		buf = make([]byte, 32*1024)
	}
	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := io.CopyBuffer(dst, src, buf)
		done <- result{n: n, err: err}
	}()
	select {
	case res := <-done:
		return res.n, res.err
	case <-ctx.Done():
		if closer, ok := src.(io.Closer); ok {
			_ = closer.Close()
		}
		res := <-done
		if res.err != nil {
			return res.n, res.err
		}
		return res.n, ctx.Err()
	}
}

func PumpGGA(ctx context.Context, rover *Rover, interval time.Duration, sentences <-chan string) error {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var latest string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sentence, ok := <-sentences:
			if !ok {
				return nil
			}
			latest = sentence
			if err := rover.WriteGGA(latest); err != nil {
				return err
			}
		case <-ticker.C:
			if latest != "" {
				if err := rover.WriteGGA(latest); err != nil {
					return err
				}
			}
		}
	}
}
