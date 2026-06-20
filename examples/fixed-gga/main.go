package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	ntrip "github.com/peinser/go-ntrip"
)

func main() {
	var lat, lon, alt, hdop float64
	var satellites int
	flag.Float64Var(&lat, "lat", 0, "latitude in degrees")
	flag.Float64Var(&lon, "lon", 0, "longitude in degrees")
	flag.Float64Var(&alt, "alt", 0, "altitude in meters")
	flag.Float64Var(&hdop, "hdop", 1.0, "HDOP")
	flag.IntVar(&satellites, "satellites", 12, "satellite count")
	flag.Parse()

	sentence, err := ntrip.FormatGGA(ntrip.GGA{
		Time:           time.Now().UTC(),
		Latitude:       lat,
		Longitude:      lon,
		FixQuality:     ntrip.FixGPS,
		Satellites:     satellites,
		HDOP:           hdop,
		AltitudeMeters: alt,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := ntrip.ValidateGGA(sentence); err != nil {
		log.Fatal(err)
	}
	fmt.Println(sentence)
}
