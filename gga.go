package ntrip

import (
	"fmt"
	"math"
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
)

type GGA struct {
	Time              time.Time
	Latitude          float64
	Longitude         float64
	FixQuality        GGAFixQuality
	Satellites        int
	HDOP              float64
	AltitudeMeters    float64
	GeoidHeightMeters float64
	StationID         string
	Talker            string
}

func FormatGGA(gga GGA) (string, error) {
	if math.IsNaN(gga.Latitude) || math.IsNaN(gga.Longitude) || gga.Latitude < -90 || gga.Latitude > 90 || gga.Longitude < -180 || gga.Longitude > 180 {
		return "", fmt.Errorf("%w: invalid GGA latitude/longitude", ErrInvalidConfig)
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
	return "$" + body + "*" + checksum(body), nil
}

func ValidateGGA(sentence string) error {
	sentence = normalizeSentence(sentence)
	if !strings.HasPrefix(sentence, "$") {
		return fmt.Errorf("%w: GGA sentence must start with $", ErrInvalidConfig)
	}
	star := strings.LastIndex(sentence, "*")
	if star < 0 || star+3 != len(sentence) {
		return fmt.Errorf("%w: GGA sentence checksum is required", ErrInvalidConfig)
	}
	body := sentence[1:star]
	if len(body) < 5 || body[2:5] != "GGA" {
		return fmt.Errorf("%w: not a GGA sentence", ErrInvalidConfig)
	}
	if !strings.EqualFold(sentence[star+1:], checksum(body)) {
		return fmt.Errorf("%w: invalid GGA checksum", ErrInvalidConfig)
	}
	return nil
}

func normalizeSentence(sentence string) string {
	return strings.TrimRight(strings.TrimSpace(sentence), "\r\n")
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
	abs := math.Abs(value)
	deg := math.Floor(abs)
	min := (abs - deg) * 60
	if lat {
		return fmt.Sprintf("%02.0f%07.4f", deg, min), hemi
	}
	return fmt.Sprintf("%03.0f%07.4f", deg, min), hemi
}

func checksum(body string) string {
	var sum byte
	for i := 0; i < len(body); i++ {
		sum ^= body[i]
	}
	return fmt.Sprintf("%02X", sum)
}
