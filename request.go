package ntrip

import (
	"encoding/base64"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"
)

func writeRequest(conn net.Conn, timeout time.Duration, method string, ep endpoint, userAgent string, credentials Credentials, headers map[string]string, extra map[string]string) error {
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
		if cleanHeader(key, value) {
			merged[key] = value
		}
	}
	for key, value := range headers {
		if strings.EqualFold(key, "Host") || strings.EqualFold(key, "Authorization") {
			continue
		}
		if cleanHeader(key, value) {
			merged[key] = value
		}
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
	return writeFull(conn, []byte(builder.String()), timeout)
}

func cleanHeader(key, value string) bool {
	return key != "" && !strings.ContainsAny(key, "\r\n:") && !strings.ContainsAny(value, "\r\n")
}

func expectOK(resp response) error {
	if resp.Code >= 200 && resp.Code <= 299 {
		return nil
	}
	return &StatusError{Code: resp.Code, Status: resp.Status}
}
