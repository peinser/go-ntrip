package ntrip

import (
	"crypto/tls"
	"log/slog"
	"math"
	"net"
	"time"
)

const (
	DefaultUserAgent           = "NTRIP go-ntrip/0.1"
	DefaultDialTimeout         = 10 * time.Second
	DefaultHeaderTimeout       = 10 * time.Second
	DefaultWriteTimeout        = 5 * time.Second
	DefaultReadIdleTimeout     = 60 * time.Second
	DefaultMaxHeaderBytes      = 64 * 1024
	DefaultMaxSourcetableBytes = 8 * 1024 * 1024
	DefaultBodyTimeout         = 30 * time.Second
	DefaultReconnectMin        = 500 * time.Millisecond
	DefaultReconnectMax        = 30 * time.Second
)

type Credentials struct {
	Username string
	Password string
}

type RoverConfig struct {
	URL            string
	Credentials    Credentials
	UserAgent      string
	DialTimeout    time.Duration
	HeaderTimeout  time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes uint64
	TLSConfig      *tls.Config
	Dialer         *net.Dialer
	Headers        map[string]string
}

type SourceConfig struct {
	URL            string
	Credentials    Credentials
	UserAgent      string
	DialTimeout    time.Duration
	HeaderTimeout  time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes uint64
	TLSConfig      *tls.Config
	Dialer         *net.Dialer
	Headers        map[string]string
}

type SourcetableConfig struct {
	URL            string
	Credentials    Credentials
	UserAgent      string
	DialTimeout    time.Duration
	HeaderTimeout  time.Duration
	WriteTimeout   time.Duration
	MaxHeaderBytes uint64
	TLSConfig      *tls.Config
	Dialer         *net.Dialer
	Headers        map[string]string
	// BodyTimeout bounds the whole download, not just idle time between reads.
	BodyTimeout  time.Duration
	MaxBodyBytes int64
}

type BackoffConfig struct {
	Min    time.Duration
	Max    time.Duration
	Factor float64
	// ResetAfter is the minimum healthy session duration before resetting backoff.
	ResetAfter time.Duration
}

type StreamConfig struct {
	Rover           RoverConfig
	GGAInterval     time.Duration
	ReadIdleTimeout time.Duration
	Reconnect       BackoffConfig
	Logger          *slog.Logger
	BufferSize      int
	ReconnectOnEOF  bool
}

type SourceStreamConfig struct {
	Source     SourceConfig
	Reconnect  BackoffConfig
	Logger     *slog.Logger
	BufferSize int
}

func (b BackoffConfig) normalized() BackoffConfig {
	if b.Min <= 0 {
		b.Min = DefaultReconnectMin
	}
	if b.Max <= 0 {
		b.Max = DefaultReconnectMax
	}
	if b.Max < b.Min {
		b.Max = b.Min
	}
	if !finite(b.Factor) || b.Factor < 1.1 {
		b.Factor = 2
	}
	if b.ResetAfter <= 0 {
		b.ResetAfter = 30 * time.Second
	}
	return b
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func userAgent(value string) string {
	if value != "" {
		return value
	}
	return DefaultUserAgent
}

func dialTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return DefaultDialTimeout
}

func headerTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return DefaultHeaderTimeout
}

func writeTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return DefaultWriteTimeout
}

func readIdleTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return DefaultReadIdleTimeout
}

func maxHeaderBytes(value uint64) uint64 {
	if value > 0 {
		return value
	}
	return DefaultMaxHeaderBytes
}

func bufferSize(value int) int {
	if value > 0 {
		return value
	}
	return 32 * 1024
}
