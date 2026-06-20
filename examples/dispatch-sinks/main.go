package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	ntrip "github.com/peinser/go-ntrip"
)

func main() {
	var url, username, password, udpAddr, filePath, devicePath string
	var stdout bool
	var lat, lon, alt float64
	flag.StringVar(&url, "url", "", "NTRIP rover URL")
	flag.StringVar(&username, "username", "", "NTRIP username")
	flag.StringVar(&password, "password", "", "NTRIP password")
	flag.StringVar(&udpAddr, "udp", "", "optional UDP RTCM sink")
	flag.StringVar(&filePath, "file", "", "optional file RTCM sink")
	flag.StringVar(&devicePath, "device", "", "optional raw RTCM device sink, for example /dev/ttyACM0")
	flag.BoolVar(&stdout, "stdout", false, "write raw RTCM to stdout")
	flag.Float64Var(&lat, "lat", 0, "rover latitude in degrees")
	flag.Float64Var(&lon, "lon", 0, "rover longitude in degrees")
	flag.Float64Var(&alt, "alt", 0, "rover altitude in meters")
	flag.Parse()
	if url == "" {
		log.Fatal("-url is required")
	}

	sinks, closeSinks, err := openSinks(udpAddr, filePath, devicePath, stdout)
	if err != nil {
		log.Fatal(err)
	}
	defer closeSinks()
	if len(sinks) == 0 {
		log.Fatal("at least one sink is required: -udp, -file, -device, or -stdout")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stream := ntrip.NewRoverStream(ntrip.StreamConfig{
		Rover:           ntrip.RoverConfig{URL: url, Credentials: ntrip.Credentials{Username: username, Password: password}},
		GGAInterval:     5 * time.Second,
		ReadIdleTimeout: 60 * time.Second,
		ReconnectOnEOF:  true,
	})
	sentence, err := ntrip.FormatGGA(ntrip.GGA{Time: time.Now().UTC(), Latitude: lat, Longitude: lon, AltitudeMeters: alt, FixQuality: ntrip.FixGPS, Satellites: 12, HDOP: 1.0})
	if err != nil {
		log.Fatalf("format gga: %v", err)
	}
	if err := stream.SetGGA(sentence); err != nil {
		log.Fatalf("set gga: %v", err)
	}

	writer := io.MultiWriter(sinks...)
	if err := stream.RunToWriter(ctx, writer); err != nil && ctx.Err() == nil {
		log.Fatalf("ntrip stream stopped: %v", err)
	}
}

func openSinks(udpAddr, filePath, devicePath string, stdout bool) ([]io.Writer, func(), error) {
	var sinks []io.Writer
	var closers []io.Closer
	if udpAddr != "" {
		conn, err := net.Dial("udp", udpAddr)
		if err != nil {
			return nil, nil, err
		}
		sinks = append(sinks, conn)
		closers = append(closers, conn)
	}
	if filePath != "" {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, nil, err
		}
		sinks = append(sinks, file)
		closers = append(closers, file)
	}
	if devicePath != "" {
		device, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
		if err != nil {
			return nil, nil, err
		}
		sinks = append(sinks, device)
		closers = append(closers, device)
	}
	if stdout {
		sinks = append(sinks, os.Stdout)
	}
	return sinks, func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}, nil
}
