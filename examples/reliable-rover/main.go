package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os/signal"
	"syscall"
	"time"

	ntrip "github.com/peinser/go-ntrip"
)

func main() {
	var url, username, password, udpAddr string
	var lat, lon, alt float64
	flag.StringVar(&url, "url", "", "NTRIP rover URL, for example https://caster.example/*")
	flag.StringVar(&username, "username", "", "NTRIP username")
	flag.StringVar(&password, "password", "", "NTRIP password")
	flag.StringVar(&udpAddr, "udp", "127.0.0.1:13320", "UDP address that receives raw RTCM bytes")
	flag.Float64Var(&lat, "lat", 0, "rover latitude in degrees")
	flag.Float64Var(&lon, "lon", 0, "rover longitude in degrees")
	flag.Float64Var(&alt, "alt", 0, "rover altitude in meters")
	flag.Parse()
	if url == "" {
		log.Fatal("-url is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		log.Fatalf("dial udp sink: %v", err)
	}

	stream := ntrip.NewRoverStream(ntrip.StreamConfig{
		Rover: ntrip.RoverConfig{
			URL:         url,
			Credentials: ntrip.Credentials{Username: username, Password: password},
		},
		GGAInterval:     5 * time.Second,
		ReadIdleTimeout: 60 * time.Second,
		Reconnect:       ntrip.BackoffConfig{Min: time.Second, Max: 30 * time.Second, Factor: 2},
		ReconnectOnEOF:  true,
	})

	initial, err := ntrip.FormatGGA(ntrip.GGA{Latitude: lat, Longitude: lon, AltitudeMeters: alt, FixQuality: ntrip.FixGPS, Satellites: 12, HDOP: 1.0})
	if err != nil {
		_ = conn.Close()
		log.Fatal(err)
	}
	if err := stream.SetGGA(initial); err != nil {
		_ = conn.Close()
		log.Fatal(err)
	}
	updatesCtx, stopUpdates := context.WithCancel(ctx)
	updatesDone := make(chan struct{})
	go func() { defer close(updatesDone); updateFixedGGA(updatesCtx, stream, lat, lon, alt, 5*time.Second) }()

	log.Printf("streaming RTCM from %s to udp://%s", url, udpAddr)
	err = stream.RunToWriter(ctx, conn)
	stopUpdates()
	<-updatesDone
	if err != nil && ctx.Err() == nil {
		log.Fatalf("ntrip stream stopped: %v", err)
	}
	stats := stream.Stats()
	log.Printf("stopped: connections=%d reconnects=%d bytes_read=%d bytes_written=%d", stats.Connections, stats.Reconnects, stats.BytesRead, stats.BytesWritten)
}

func updateFixedGGA(ctx context.Context, stream *ntrip.RoverStream, lat, lon, alt float64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		sentence, err := ntrip.FormatGGA(ntrip.GGA{
			Time:           time.Now().UTC(),
			Latitude:       lat,
			Longitude:      lon,
			FixQuality:     ntrip.FixGPS,
			Satellites:     12,
			HDOP:           1.0,
			AltitudeMeters: alt,
		})
		if err != nil {
			log.Printf("format gga: %v", err)
		} else if err := stream.SetGGA(sentence); err != nil {
			log.Printf("set gga: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
