package ntrip

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type GGAFixQuality int

const (
	FixInvalid GGAFixQuality = iota
	FixGPS
	FixDGPS
	FixPPS
	FixRTK
	FixFloatRTK
	FixEstimated
	FixManual
	FixSimulation
)

type GGA struct {
	Time              time.Time
	Latitude          float64
	Longitude         float64
	FixQuality        GGAFixQuality
	Satellites        int
	HDOP              float64
	AltitudeMeters    float64 // Mean sea level (orthometric), not ellipsoid height.
	GeoidHeightMeters float64
	StationID         string
	Talker            string
}

func FormatGGA(gga GGA) (string, error) {
	if !validPosition(gga.Latitude, gga.Longitude) {
		return "", fmt.Errorf("%w: invalid GGA latitude/longitude", ErrInvalidConfig)
	}
	if !finite(gga.HDOP) || !finite(gga.AltitudeMeters) || !finite(gga.GeoidHeightMeters) || gga.HDOP < 0 || gga.Satellites < 0 || gga.Satellites > 99 || gga.FixQuality < FixInvalid || gga.FixQuality > FixSimulation {
		return "", fmt.Errorf("%w: invalid GGA fix quality, satellites, HDOP, or height", ErrInvalidConfig)
	}
	talker := gga.Talker
	if talker == "" {
		talker = "GN"
	}
	if len(talker) != 2 {
		return "", fmt.Errorf("%w: GGA talker must be two characters", ErrInvalidConfig)
	}
	stamp := gga.Time.UTC()
	if stamp.IsZero() {
		stamp = time.Now().UTC()
	}
	lat, ns := formatNMEACoord(gga.Latitude, true)
	lon, ew := formatNMEACoord(gga.Longitude, false)
	body := fmt.Sprintf("%sGGA,%s,%s,%s,%s,%s,%d,%02d,%.1f,%.1f,M,%.1f,M,,%s",
		strings.ToUpper(talker),
		stamp.Format("150405.00"),
		lat,
		ns,
		lon,
		ew,
		gga.FixQuality,
		gga.Satellites,
		gga.HDOP,
		gga.AltitudeMeters,
		gga.GeoidHeightMeters,
		gga.StationID,
	)
	sentence := "$" + body + "*" + checksum(body)
	if err := ValidateGGA(sentence); err != nil {
		return "", err
	}
	return sentence, nil
}

func ValidateGGA(sentence string) error {
	sentence = normalizeSentence(sentence)
	if len(sentence)+2 > 82 {
		return fmt.Errorf("%w: GGA exceeds the NMEA 82-byte limit including CRLF", ErrInvalidConfig)
	}
	if !strings.HasPrefix(sentence, "$") {
		return fmt.Errorf("%w: GGA sentence must start with $", ErrInvalidConfig)
	}
	star := strings.LastIndex(sentence, "*")
	if star < 0 || star+3 != len(sentence) {
		return fmt.Errorf("%w: GGA sentence checksum is required", ErrInvalidConfig)
	}
	body := sentence[1:star]
	for i := range body {
		if body[i] < 32 || body[i] > 126 || body[i] == '$' || body[i] == '*' {
			return fmt.Errorf("%w: invalid character in GGA", ErrInvalidConfig)
		}
	}
	fields := strings.Split(body, ",")
	if len(fields) != 15 || len(fields[0]) != 5 || fields[0][2:] != "GGA" || fields[0][0] < 'A' || fields[0][0] > 'Z' || fields[0][1] < 'A' || fields[0][1] > 'Z' {
		return fmt.Errorf("%w: not a GGA sentence", ErrInvalidConfig)
	}
	if !strings.EqualFold(sentence[star+1:], checksum(body)) {
		return fmt.Errorf("%w: invalid GGA checksum", ErrInvalidConfig)
	}
	if len(fields[6]) != 1 || fields[6][0] < '0' || fields[6][0] > '8' {
		return fmt.Errorf("%w: invalid GGA fix quality", ErrInvalidConfig)
	}
	hasFix := fields[6] != "0"
	if fields[1] != "" || hasFix {
		integer, fraction, dot := strings.Cut(fields[1], ".")
		if len(integer) != 6 || !digits(integer) || (dot && !digits(fraction)) {
			return fmt.Errorf("%w: invalid GGA UTC time", ErrInvalidConfig)
		}
		h, _ := strconv.Atoi(integer[:2])
		m, _ := strconv.Atoi(integer[2:4])
		s, _ := strconv.Atoi(integer[4:])
		if h > 23 || m > 59 || s > 60 || (s == 60 && (h != 23 || m != 59)) {
			return fmt.Errorf("%w: invalid GGA UTC time", ErrInvalidConfig)
		}
	}
	if hasFix || fields[2] != "" || fields[3] != "" || fields[4] != "" || fields[5] != "" {
		if !validNMEACoord(fields[2], 2, 90) || (fields[3] != "N" && fields[3] != "S") || !validNMEACoord(fields[4], 3, 180) || (fields[5] != "E" && fields[5] != "W") {
			return fmt.Errorf("%w: invalid GGA coordinates", ErrInvalidConfig)
		}
	}
	if fields[7] != "" || hasFix {
		if len(fields[7]) != 2 || !digits(fields[7]) {
			return fmt.Errorf("%w: GGA satellites must be 00..99", ErrInvalidConfig)
		}
	}
	for _, index := range []int{8, 9, 11, 13} {
		if fields[index] == "" {
			if hasFix && (index == 8 || index == 9) {
				return fmt.Errorf("%w: missing GGA field %d", ErrInvalidConfig, index)
			}
			continue
		}
		value, err := nmeaDecimal(fields[index], index == 9 || index == 11)
		if err != nil || ((index == 8 || index == 13) && value < 0) {
			return fmt.Errorf("%w: invalid numeric GGA field %d", ErrInvalidConfig, index)
		}
	}
	for _, index := range []int{10, 12} {
		if fields[index] != "M" && !(fields[index] == "" && fields[index-1] == "") {
			return fmt.Errorf("%w: GGA heights must use meters", ErrInvalidConfig)
		}
	}
	if station := fields[14]; station != "" {
		id, err := strconv.Atoi(station)
		if len(station) > 4 || !digits(station) || err != nil || id > 4095 {
			return fmt.Errorf("%w: invalid GGA station ID", ErrInvalidConfig)
		}
	}
	return nil
}

func normalizeSentence(sentence string) string {
	return strings.TrimSuffix(sentence, "\r\n")
}

func digits(value string) bool {
	if value == "" {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func nmeaDecimal(value string, signed bool) (float64, error) {
	unsigned := value
	if signed && len(unsigned) > 0 && (unsigned[0] == '-' || unsigned[0] == '+') {
		unsigned = unsigned[1:]
	}
	integer, fraction, dot := strings.Cut(unsigned, ".")
	if !digits(integer) || (dot && !digits(fraction)) {
		return 0, fmt.Errorf("invalid decimal")
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || !finite(parsed) {
		return 0, fmt.Errorf("invalid decimal")
	}
	return parsed, nil
}

func validNMEACoord(value string, degreeDigits int, limit float64) bool {
	integer, _, _ := strings.Cut(value, ".")
	if len(integer) != degreeDigits+2 || !digits(integer) {
		return false
	}
	deg, _ := strconv.Atoi(integer[:degreeDigits])
	minutes, err := nmeaDecimal(value[degreeDigits:], false)
	return err == nil && minutes < 60 && float64(deg) <= limit && (float64(deg) != limit || minutes == 0)
}

func formatNMEACoord(value float64, lat bool) (string, string) {
	hemi := "N"
	if lat {
		if value < 0 {
			hemi = "S"
		}
	} else {
		hemi = "E"
		if value < 0 {
			hemi = "W"
		}
	}
	// Round total minutes before splitting degrees, so 59.99995 carries correctly.
	units := int64(math.Round(math.Abs(value) * 60 * 10000))
	deg := units / 600000
	minutes := units % 600000
	if lat {
		return fmt.Sprintf("%02d%02d.%04d", deg, minutes/10000, minutes%10000), hemi
	}
	return fmt.Sprintf("%03d%02d.%04d", deg, minutes/10000, minutes%10000), hemi
}

func checksum(body string) string {
	var sum byte
	for i := 0; i < len(body); i++ {
		sum ^= body[i]
	}
	return fmt.Sprintf("%02X", sum)
}
