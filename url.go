package ntrip

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

type endpoint struct {
	URL       *url.URL
	Network   string
	Address   string
	Host      string
	PathQuery string
	TLS       bool
}

func parseEndpoint(raw string) (endpoint, error) {
	if strings.TrimSpace(raw) == "" {
		return endpoint{}, fmt.Errorf("%w: url is required", ErrInvalidConfig)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return endpoint{}, fmt.Errorf("%w: parse url: %v", ErrInvalidConfig, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return endpoint{}, fmt.Errorf("%w: unsupported url scheme %q", ErrInvalidConfig, u.Scheme)
	}
	if u.Hostname() == "" || u.Opaque != "" {
		return endpoint{}, fmt.Errorf("%w: url host is required", ErrInvalidConfig)
	}
	if u.User != nil {
		return endpoint{}, fmt.Errorf("%w: credentials in url are not supported", ErrInvalidConfig)
	}
	if u.Fragment != "" {
		return endpoint{}, fmt.Errorf("%w: URL fragments are not supported", ErrInvalidConfig)
	}
	pathQuery := u.RequestURI()
	if pathQuery == "" {
		pathQuery = "/"
	}
	for i := range pathQuery {
		if pathQuery[i] <= 32 || pathQuery[i] >= 127 {
			return endpoint{}, fmt.Errorf("%w: URL request target must be percent-encoded ASCII", ErrInvalidConfig)
		}
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "2101"
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return endpoint{}, fmt.Errorf("%w: invalid URL port %q", ErrInvalidConfig, port)
	}
	address := net.JoinHostPort(u.Hostname(), port)
	return endpoint{
		URL:       u,
		Network:   "tcp",
		Address:   address,
		Host:      u.Host,
		PathQuery: pathQuery,
		TLS:       u.Scheme == "https",
	}, nil
}
