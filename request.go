package ntrip

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

func requestBytes(method string, ep endpoint, userAgent string, credentials Credentials, headers map[string]string, extra map[string]string) ([]byte, error) {
	if !cleanHeader("User-Agent", userAgent) || !cleanHeader("Host", ep.Host) || strings.Contains(credentials.Username, ":") {
		return nil, fmt.Errorf("%w: invalid user agent, host, or Basic authentication username", ErrInvalidConfig)
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s %s HTTP/1.1\r\n", method, ep.PathQuery)
	merged := map[string]string{
		"Host":          ep.Host,
		"User-Agent":    userAgent,
		"Ntrip-Version": "Ntrip/2.0",
		"Connection":    "close",
	}
	if credentials.Username != "" || credentials.Password != "" {
		value := credentials.Username + ":" + credentials.Password
		merged["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(value))
	}
	for key, value := range extra {
		merged[key] = value
	}
	for key, value := range headers {
		if !cleanHeader(key, value) {
			return nil, fmt.Errorf("%w: invalid header %q", ErrInvalidConfig, key)
		}
		name := http.CanonicalHeaderKey(key)
		if name == "Ntrip-Gga" {
			if err := ValidateGGA(value); err != nil {
				return nil, fmt.Errorf("Ntrip-GGA header: %w", err)
			}
		}
		switch name {
		case "Host", "Authorization", "User-Agent", "Ntrip-Version", "Connection", "Transfer-Encoding", "Content-Length", "Content-Type", "Content-Encoding", "Trailer", "Expect", "Upgrade":
			return nil, fmt.Errorf("%w: header %q is managed by the transport", ErrInvalidConfig, key)
		}
		if _, exists := merged[name]; exists {
			return nil, fmt.Errorf("%w: duplicate header %q", ErrInvalidConfig, key)
		}
		merged[name] = value
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(&builder, "%s: %s\r\n", key, merged[key])
	}
	builder.WriteString("\r\n")
	return []byte(builder.String()), nil
}

func cleanHeader(key, value string) bool {
	if key == "" {
		return false
	}
	for i := range key {
		c := key[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c))) {
			return false
		}
	}
	for i := range value {
		if (value[i] < 32 && value[i] != '\t') || value[i] == 127 {
			return false
		}
	}
	return true
}

func expectOK(resp response) error {
	if resp.Code == http.StatusOK {
		return nil
	}
	return &StatusError{Code: resp.Code, Status: resp.Status}
}
