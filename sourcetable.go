package ntrip

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	earthRadiusMeters = 6371010.0
	// Bound parsed-object amplification, not just the downloaded byte count.
	maxSourcetableRecords  = 100_000
	maxSourcetableWarnings = 1_000
)

type Sourcetable struct {
	Casters  []CasterEntry
	Networks []NetworkEntry
	Streams  []StreamEntry
	Raw      []string
	Header   map[string][]string
	Warnings []ParseWarning
	Unknown  []string
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
	Extra        []string
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
	Extra               []string
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
	Extra          []string
}

type StreamDistance struct {
	Stream StreamEntry
	Meters float64
}

func FetchSourcetable(ctx context.Context, cfg SourcetableConfig) (Sourcetable, error) {
	if cfg.MaxBodyBytes == math.MaxInt64 {
		return Sourcetable{}, fmt.Errorf("%w: sourcetable limit is too large", ErrInvalidConfig)
	}
	ep, err := parseEndpoint(cfg.URL)
	if err != nil {
		return Sourcetable{}, err
	}
	request, err := requestBytes("GET", ep, userAgent(cfg.UserAgent), cfg.Credentials, cfg.Headers, nil)
	if err != nil {
		return Sourcetable{}, err
	}
	conn, err := dial(ctx, ep, dialTimeout(cfg.DialTimeout), cfg.Dialer, cfg.TLSConfig)
	if err != nil {
		return Sourcetable{}, err
	}
	closeConn := closeWithContext(ctx, conn)
	defer closeConn()
	if _, err := writeFull(conn, request, writeTimeout(cfg.WriteTimeout)); err != nil {
		if ctx.Err() != nil {
			return Sourcetable{}, ctx.Err()
		}
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
		if ctx.Err() != nil {
			return Sourcetable{}, ctx.Err()
		}
		return Sourcetable{}, err
	}
	if err := expectOK(resp); err != nil {
		return Sourcetable{}, err
	}
	timeout := cfg.BodyTimeout
	if timeout <= 0 {
		timeout = DefaultBodyTimeout
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return Sourcetable{}, err
	}
	limit := cfg.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxSourcetableBytes
	}
	// Avoid limit+1 overflow while keeping one byte to distinguish exact limits.
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if ctx.Err() != nil {
		return Sourcetable{}, ctx.Err()
	}
	if err != nil {
		return Sourcetable{}, err
	}
	if int64(len(data)) > limit {
		return Sourcetable{}, fmt.Errorf("%w: sourcetable exceeds %d bytes", ErrBadResponse, limit)
	}
	st, err := ParseSourcetable(string(data))
	if err != nil {
		return Sourcetable{}, err
	}
	st.Header = resp.Header
	return st, nil
}

func ParseSourcetable(input string) (Sourcetable, error) {
	var st Sourcetable
	scanner := bufio.NewScanner(strings.NewReader(input))
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	lineNo := 0
	ended := false
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if line == "" {
			continue
		}
		if ended {
			return Sourcetable{}, fmt.Errorf("%w: data after ENDSOURCETABLE", ErrBadResponse)
		}
		if line == "ENDSOURCETABLE" {
			ended = true
			continue
		}
		st.Raw = append(st.Raw, line)
		if len(st.Raw) > maxSourcetableRecords {
			return Sourcetable{}, fmt.Errorf("%w: sourcetable exceeds %d records", ErrBadResponse, maxSourcetableRecords)
		}
		kind, _, _ := strings.Cut(line, ";")
		switch kind {
		case "CAS":
			entry, warnings := parseCaster(strings.Split(line, ";"), line, lineNo)
			st.Casters = append(st.Casters, entry)
			st.Warnings = append(st.Warnings, warnings...)
		case "NET":
			entry, warnings := parseNetwork(strings.Split(line, ";"), line, lineNo)
			st.Networks = append(st.Networks, entry)
			st.Warnings = append(st.Warnings, warnings...)
		case "STR":
			entry, warnings := parseStream(strings.Split(line, ";"), line, lineNo)
			st.Streams = append(st.Streams, entry)
			st.Warnings = append(st.Warnings, warnings...)
		default:
			st.Unknown = append(st.Unknown, line)
			st.Warnings = append(st.Warnings, ParseWarning{Line: lineNo, Field: "record", Value: kind, Err: fmt.Errorf("unknown sourcetable record")})
		}
		if len(st.Warnings) > maxSourcetableWarnings {
			return Sourcetable{}, fmt.Errorf("%w: sourcetable exceeds %d parse warnings", ErrBadResponse, maxSourcetableWarnings)
		}
	}
	if err := scanner.Err(); err != nil {
		return Sourcetable{}, err
	}
	if !ended {
		return Sourcetable{}, fmt.Errorf("%w: missing ENDSOURCETABLE", ErrBadResponse)
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
	lines = append(lines, st.Unknown...)
	lines = append(lines, "ENDSOURCETABLE")
	return strings.Join(lines, "\r\n") + "\r\n"
}

// RawString returns the original records in their original order, including
// malformed/unknown fields. String instead serializes the current typed values.
// Both methods normalize line endings to CRLF and include the table terminator.
func (st Sourcetable) RawString() string {
	return strings.Join(append(append([]string(nil), st.Raw...), "ENDSOURCETABLE"), "\r\n") + "\r\n"
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
	if !validPosition(latitude, longitude) || !finite(maxMeters) {
		return nil
	}
	out := make([]StreamDistance, 0, len(st.Streams))
	for _, stream := range st.Streams {
		if !validPosition(stream.Latitude, stream.Longitude) {
			continue
		}
		meters := DistanceMeters(latitude, longitude, stream.Latitude, stream.Longitude)
		if maxMeters <= 0 || meters <= maxMeters {
			out = append(out, StreamDistance{Stream: stream, Meters: meters})
		}
	}
	slices.SortStableFunc(out, func(a, b StreamDistance) int { return cmp.Compare(a.Meters, b.Meters) })
	return out
}

func DistanceMeters(latA, lonA, latB, lonB float64) float64 {
	if !validPosition(latA, lonA) || !validPosition(latB, lonB) {
		return math.NaN()
	}
	lat1 := latA * math.Pi / 180
	lat2 := latB * math.Pi / 180
	lon1 := lonA * math.Pi / 180
	lon2 := lonB * math.Pi / 180
	dLat := lat2 - lat1
	dLon := lon2 - lon1
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
	a = max(0, min(1, a))
	return earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func validPosition(latitude, longitude float64) bool {
	return finite(latitude) && finite(longitude) && latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180
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
		Latitude:     p.coordinate(7, "latitude", 90),
		Longitude:    p.coordinate(8, "longitude", 180),
		FallbackHost: p.string(9, "fallback_host"),
		FallbackPort: p.int(10, "fallback_port"),
		Misc:         p.string(11, "misc"),
		Raw:          raw,
		Extra:        fields[min(len(fields), 12):],
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
		Extra:               fields[min(len(fields), 9):],
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
		Latitude:       p.coordinate(9, "latitude", 90),
		Longitude:      p.coordinate(10, "longitude", 180),
		NMEA:           p.bool(11, "nmea", "0"),
		Solution:       p.bool(12, "solution", "0"),
		Generator:      p.string(13, "generator"),
		Compression:    p.string(14, "compression"),
		Authentication: p.string(15, "authentication"),
		Fee:            p.bool(16, "fee", "N"),
		Bitrate:        p.int(17, "bitrate"),
		Misc:           p.string(18, "misc"),
		Raw:            raw,
		Extra:          fields[min(len(fields), 19):],
	}
	return entry, p.warnings
}

func (c CasterEntry) String() string {
	return strings.Join(append([]string{"CAS", c.Host, strconv.Itoa(c.Port), c.Identifier, c.Operator, formatBool01(c.NMEA), c.Country, formatFloat(c.Latitude), formatFloat(c.Longitude), c.FallbackHost, strconv.Itoa(c.FallbackPort), c.Misc}, c.Extra...), ";")
}

func (n NetworkEntry) String() string {
	return strings.Join(append([]string{"NET", n.Identifier, n.Operator, n.Authentication, formatBoolYN(n.Fee), n.NetworkInfoURL, n.StreamInfoURL, n.RegistrationAddress, n.Misc}, n.Extra...), ";")
}

func (s StreamEntry) String() string {
	return strings.Join(append([]string{"STR", s.Mountpoint, s.Identifier, s.Format, s.FormatDetails, s.Carrier, s.NavSystem, s.Network, s.Country, formatFloat(s.Latitude), formatFloat(s.Longitude), formatBool01(s.NMEA), formatBool01(s.Solution), s.Generator, s.Compression, s.Authentication, formatBoolYN(s.Fee), strconv.Itoa(s.Bitrate), s.Misc}, s.Extra...), ";")
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
	if parsed < 0 || ((name == "port" || name == "fallback_port") && parsed > 65535) || (name == "port" && parsed == 0) {
		p.warn(name, value, fmt.Errorf("integer out of range"))
		return 0
	}
	return parsed
}

func (p *parser) coordinate(index int, name string, limit float64) float64 {
	value := p.string(index, name)
	if value == "" {
		p.warn(name, value, fmt.Errorf("missing coordinate"))
		return math.NaN()
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || !finite(parsed) || math.Abs(parsed) > limit {
		p.warn(name, value, fmt.Errorf("invalid coordinate"))
		return math.NaN()
	}
	return parsed
}

func (p *parser) bool(index int, name string, falseValue string) bool {
	value := p.string(index, name)
	if value == "" {
		p.warn(name, value, fmt.Errorf("missing boolean"))
		return false
	}
	trueValue := "1"
	if falseValue == "N" {
		trueValue = "Y"
	}
	switch strings.ToUpper(value) {
	case falseValue:
		return false
	case trueValue:
		return true
	default:
		p.warn(name, value, fmt.Errorf("expected %s or %s", falseValue, trueValue))
		return false
	}
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
	if !finite(value) {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}
