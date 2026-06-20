package ntrip

import (
	"fmt"
	"net/url"
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
	if u.Host == "" {
		return endpoint{}, fmt.Errorf("%w: url host is required", ErrInvalidConfig)
	}
	if u.User != nil {
		return endpoint{}, fmt.Errorf("%w: credentials in url are not supported", ErrInvalidConfig)
	}
	pathQuery := u.RequestURI()
	if pathQuery == "" {
		pathQuery = "/"
	}
	address := u.Host
	if !strings.Contains(address, ":") {
		if u.Scheme == "https" {
			address += ":443"
		} else {
			address += ":2101"
		}
	}
	return endpoint{
		URL:       u,
		Network:   "tcp",
		Address:   address,
		Host:      u.Host,
		PathQuery: pathQuery,
		TLS:       u.Scheme == "https",
	}, nil
}
