package ntrip

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http/httputil"
	"strconv"
	"strings"
)

const earthRadiusMeters = 6371010.0

type Sourcetable struct {
	Casters  []CasterEntry
	Networks []NetworkEntry
	Streams  []StreamEntry
	Raw      []string
	Header   map[string][]string
	Warnings []ParseWarning
}

type ParseWarning struct {
	Line  int
	Field string
	Value string
	Err   error
}

func (w ParseWarning) Error() string {
	if w.Line > 0 {
		return fmt.Sprintf("line %d field %s value %q: %v", w.Line, w.Field, w.Value, w.Err)
	}
	return fmt.Sprintf("field %s value %q: %v", w.Field, w.Value, w.Err)
}

type CasterEntry struct {
	Host         string
	Port         int
	Identifier   string
	Operator     string
	NMEA         bool
	Country      string
	Latitude     float64
	Longitude    float64
	FallbackHost string
	FallbackPort int
	Misc         string
	Raw          string
}

type NetworkEntry struct {
	Identifier          string
	Operator            string
	Authentication      string
	Fee                 bool
	NetworkInfoURL      string
	StreamInfoURL       string
	RegistrationAddress string
	Misc                string
	Raw                 string
}

type StreamEntry struct {
	Mountpoint     string
	Identifier     string
	Format         string
	FormatDetails  string
	Carrier        string
	NavSystem      string
	Network        string
	Country        string
	Latitude       float64
	Longitude      float64
	NMEA           bool
	Solution       bool
	Generator      string
	Compression    string
	Authentication string
	Fee            bool
	Bitrate        int
	Misc           string
	Raw            string
}

type StreamDistance struct {
	Stream StreamEntry
	Meters float64
}

func FetchSourcetable(ctx context.Context, cfg SourcetableConfig) (Sourcetable, error) {
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return Sourcetable{}, err
	}
	conn, err := dial(ctx, ep, dialTimeout(cfg.DialTimeout), cfg.Dialer, cfg.TLSConfig)
	if err != nil {
		return Sourcetable{}, err
	}
	defer conn.Close()
	stopContextClose := closeOnContext(ctx, conn)
	defer stopContextClose()
	if err := writeRequest(conn, writeTimeout(cfg.WriteTimeout), "GET", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers, nil); err != nil {
		return Sourcetable{}, fmt.Errorf("write ntrip sourcetable request: %w", err)
	}
	reader := bufio.NewReaderSize(conn, 32*1024)
	var resp response
	err = withReadDeadline(conn, headerTimeout(cfg.HeaderTimeout), func() error {
		var err error
		resp, err = readResponse(reader, maxHeaderBytes(cfg.MaxHeaderBytes))
		return err
	})
	if err != nil {
		return Sourcetable{}, err
	}
	if err := expectOK(resp); err != nil {
		return Sourcetable{}, err
	}
	data, err := io.ReadAll(responseBodyReader(resp, reader))
	if err != nil {
		return Sourcetable{}, err
	}
	st, err := ParseSourcetable(string(data))
	if err != nil {
		return Sourcetable{}, err
	}
	st.Header = resp.Header
	return st, nil
}

func responseBodyReader(resp response, reader io.Reader) io.Reader {
	for _, value := range resp.Header["Transfer-Encoding"] {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), "chunked") {
				return httputil.NewChunkedReader(reader)
			}
		}
	}
	if values := resp.Header["Content-Length"]; len(values) > 0 {
		if n, err := strconv.ParseInt(values[0], 10, 64); err == nil && n >= 0 {
			return io.LimitReader(reader, n)
		}
	}
	return reader
}

func ParseSourcetable(input string) (Sourcetable, error) {
	var st Sourcetable
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line == "ENDSOURCETABLE" {
			continue
		}
		st.Raw = append(st.Raw, line)
		fields := strings.Split(line, ";")
		switch fields[0] {
		case "CAS":
			entry, warnings := parseCaster(fields, line, lineNo)
			st.Casters = append(st.Casters, entry)
			st.Warnings = append(st.Warnings, warnings...)
		case "NET":
			entry, warnings := parseNetwork(fields, line, lineNo)
			st.Networks = append(st.Networks, entry)
			st.Warnings = append(st.Warnings, warnings...)
		case "STR":
			entry, warnings := parseStream(fields, line, lineNo)
			st.Streams = append(st.Streams, entry)
			st.Warnings = append(st.Warnings, warnings...)
		default:
			st.Warnings = append(st.Warnings, ParseWarning{Line: lineNo, Field: "record", Value: fields[0], Err: fmt.Errorf("unknown sourcetable record")})
		}
	}
	if err := scanner.Err(); err != nil {
		return Sourcetable{}, err
	}
	return st, nil
}

func (st Sourcetable) String() string {
	lines := make([]string, 0, len(st.Casters)+len(st.Networks)+len(st.Streams)+1)
	for _, entry := range st.Casters {
		lines = append(lines, entry.String())
	}
	for _, entry := range st.Networks {
		lines = append(lines, entry.String())
	}
	for _, entry := range st.Streams {
		lines = append(lines, entry.String())
	}
	lines = append(lines, "ENDSOURCETABLE")
	return strings.Join(lines, "\r\n") + "\r\n"
}

func (st Sourcetable) HasStream(mountpoint string) (StreamEntry, bool) {
	for _, stream := range st.Streams {
		if stream.Mountpoint == mountpoint {
			return stream, true
		}
	}
	return StreamEntry{}, false
}

func (st Sourcetable) Network(identifier string) (NetworkEntry, bool) {
	for _, network := range st.Networks {
		if network.Identifier == identifier {
			return network, true
		}
	}
	return NetworkEntry{}, false
}

func (st Sourcetable) NearestStreams(latitude, longitude float64, maxMeters float64) []StreamDistance {
	out := make([]StreamDistance, 0, len(st.Streams))
	for _, stream := range st.Streams {
		meters := DistanceMeters(latitude, longitude, stream.Latitude, stream.Longitude)
		if maxMeters <= 0 || meters <= maxMeters {
			out = append(out, StreamDistance{Stream: stream, Meters: meters})
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Meters < out[j-1].Meters; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func DistanceMeters(latA, lonA, latB, lonB float64) float64 {
	lat1 := latA * math.Pi / 180
	lat2 := latB * math.Pi / 180
	lon1 := lonA * math.Pi / 180
	lon2 := lonB * math.Pi / 180
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func parseCaster(fields []string, raw string, line int) (CasterEntry, []ParseWarning) {
	p := parser{fields: fields, line: line}
	entry := CasterEntry{
		Host:         p.string(1, "host"),
		Port:         p.int(2, "port"),
		Identifier:   p.string(3, "identifier"),
		Operator:     p.string(4, "operator"),
		NMEA:         p.bool(5, "nmea", "0"),
		Country:      p.string(6, "country"),
		Latitude:     p.float(7, "latitude"),
		Longitude:    p.float(8, "longitude"),
		FallbackHost: p.string(9, "fallback_host"),
		FallbackPort: p.int(10, "fallback_port"),
		Misc:         p.string(11, "misc"),
		Raw:          raw,
	}
	return entry, p.warnings
}

func parseNetwork(fields []string, raw string, line int) (NetworkEntry, []ParseWarning) {
	p := parser{fields: fields, line: line}
	entry := NetworkEntry{
		Identifier:          p.string(1, "identifier"),
		Operator:            p.string(2, "operator"),
		Authentication:      p.string(3, "authentication"),
		Fee:                 p.bool(4, "fee", "N"),
		NetworkInfoURL:      p.string(5, "network_info_url"),
		StreamInfoURL:       p.string(6, "stream_info_url"),
		RegistrationAddress: p.string(7, "registration_address"),
		Misc:                p.string(8, "misc"),
		Raw:                 raw,
	}
	return entry, p.warnings
}

func parseStream(fields []string, raw string, line int) (StreamEntry, []ParseWarning) {
	p := parser{fields: fields, line: line}
	entry := StreamEntry{
		Mountpoint:     p.string(1, "mountpoint"),
		Identifier:     p.string(2, "identifier"),
		Format:         p.string(3, "format"),
		FormatDetails:  p.string(4, "format_details"),
		Carrier:        p.string(5, "carrier"),
		NavSystem:      p.string(6, "nav_system"),
		Network:        p.string(7, "network"),
		Country:        p.string(8, "country"),
		Latitude:       p.float(9, "latitude"),
		Longitude:      p.float(10, "longitude"),
		NMEA:           p.bool(11, "nmea", "0"),
		Solution:       p.bool(12, "solution", "0"),
		Generator:      p.string(13, "generator"),
		Compression:    p.string(14, "compression"),
		Authentication: p.string(15, "authentication"),
		Fee:            p.bool(16, "fee", "N"),
		Bitrate:        p.int(17, "bitrate"),
		Misc:           p.string(18, "misc"),
		Raw:            raw,
	}
	return entry, p.warnings
}

func (c CasterEntry) String() string {
	return strings.Join([]string{"CAS", c.Host, strconv.Itoa(c.Port), c.Identifier, c.Operator, formatBool01(c.NMEA), c.Country, formatFloat(c.Latitude), formatFloat(c.Longitude), c.FallbackHost, strconv.Itoa(c.FallbackPort), c.Misc}, ";")
}

func (n NetworkEntry) String() string {
	return strings.Join([]string{"NET", n.Identifier, n.Operator, n.Authentication, formatBoolYN(n.Fee), n.NetworkInfoURL, n.StreamInfoURL, n.RegistrationAddress, n.Misc}, ";")
}

func (s StreamEntry) String() string {
	return strings.Join([]string{"STR", s.Mountpoint, s.Identifier, s.Format, s.FormatDetails, s.Carrier, s.NavSystem, s.Network, s.Country, formatFloat(s.Latitude), formatFloat(s.Longitude), formatBool01(s.NMEA), formatBool01(s.Solution), s.Generator, s.Compression, s.Authentication, formatBoolYN(s.Fee), strconv.Itoa(s.Bitrate), s.Misc}, ";")
}

type parser struct {
	fields   []string
	line     int
	warnings []ParseWarning
}

func (p *parser) string(index int, name string) string {
	if index >= len(p.fields) {
		p.warn(name, "", fmt.Errorf("missing field"))
		return ""
	}
	return p.fields[index]
}

func (p *parser) int(index int, name string) int {
	value := p.string(index, name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		p.warn(name, value, err)
		return 0
	}
	return parsed
}

func (p *parser) float(index int, name string) float64 {
	value := p.string(index, name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		p.warn(name, value, err)
		return 0
	}
	return parsed
}

func (p *parser) bool(index int, name string, falseValue string) bool {
	value := p.string(index, name)
	if value == "" {
		return false
	}
	return strings.ToUpper(value) != falseValue
}

func (p *parser) warn(field, value string, err error) {
	p.warnings = append(p.warnings, ParseWarning{Line: p.line, Field: field, Value: value, Err: err})
}

func formatBool01(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func formatBoolYN(value bool) string {
	if value {
		return "Y"
	}
	return "N"
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 4, 64)
}
