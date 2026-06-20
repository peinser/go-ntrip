package ntrip

import (
	"errors"
	"fmt"
	"net"
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
	if errors.Is(err, ErrInvalidConfig) {
		return false
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.Code == 408 || status.Code == 425 || status.Code == 429 || status.Code >= 500
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return true
}

func (e *StatusError) Error() string {
	if e.Status != "" {
		return fmt.Sprintf("ntrip caster returned %s", e.Status)
	}
	return fmt.Sprintf("ntrip caster returned status %d", e.Code)
}
