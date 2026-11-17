package ntrip

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
)

var (
	ErrInvalidConfig = errors.New("invalid ntrip config")
	ErrBadResponse   = errors.New("bad ntrip response")
)

type StatusError struct {
	Code   int
	Status string
}

func Temporary(err error) bool {
	if err == nil {
		return false
	}
	var deliveryErr *DeliveryError
	if errors.As(err, &deliveryErr) {
		return false
	}
	if errors.Is(err, ErrInvalidConfig) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.Code == 408 || status.Code == 425 || status.Code == 429 || status.Code == 500 || status.Code == 502 || status.Code == 503 || status.Code == 504
	}
	var certificateErr *tls.CertificateVerificationError
	if errors.As(err, &certificateErr) {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETDOWN)
}

// DeliveryError stops an upload rather than guessing whether to replay bytes.
// Data is the consumed input block, owned by the error. Written counts payload
// bytes accepted by TCP/TLS, not bytes acknowledged or processed by the caster.
// The caller can inspect the block and decide how to restart its framed source.
type DeliveryError struct {
	Data    []byte
	Written int
	Err     error
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("ntrip source delivery uncertain (%d/%d payload bytes written): %v", e.Written, len(e.Data), e.Err)
}

func (e *DeliveryError) Unwrap() error { return e.Err }

func (e *StatusError) Error() string {
	if e.Status != "" {
		return fmt.Sprintf("ntrip caster returned %s", e.Status)
	}
	return fmt.Sprintf("ntrip caster returned status %d", e.Code)
}
