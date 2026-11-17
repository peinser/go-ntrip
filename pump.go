package ntrip

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// PumpRTCM owns both endpoints and closes them on return or cancellation. Their
// Close methods must unblock Read/Write. No copy goroutine survives this call.
// Buffer may be reused after return; WriterTo/ReaderFrom fast paths may ignore it.
func PumpRTCM(ctx context.Context, dst io.WriteCloser, src io.ReadCloser, buf []byte) (n int64, err error) {
	if dst == nil || src == nil {
		return 0, fmt.Errorf("%w: pump endpoints are required", ErrInvalidConfig)
	}
	closeSrc := closeWithContext(ctx, src)
	closeDst := closeWithContext(ctx, dst)
	defer func() {
		err = errors.Join(err, closeSrc(), closeDst())
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if len(buf) == 0 {
		buf = make([]byte, 32*1024)
	}
	return io.CopyBuffer(dst, src, buf)
}

// PumpGGA owns rover and closes it on return or cancellation. Sentences are sent
// immediately and the latest is repeated at interval until the channel closes.
func PumpGGA(ctx context.Context, rover *Rover, interval time.Duration, sentences <-chan string) (err error) {
	if rover == nil {
		return fmt.Errorf("%w: rover is required", ErrInvalidConfig)
	}
	closeRover := closeWithContext(ctx, rover)
	defer func() {
		err = errors.Join(err, closeRover())
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
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
