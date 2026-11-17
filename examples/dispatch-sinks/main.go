package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stream := ntrip.NewRoverStream(ntrip.StreamConfig{
		Rover:           ntrip.RoverConfig{URL: url, Credentials: ntrip.Credentials{Username: username, Password: password}},
		GGAInterval:     5 * time.Second,
		ReadIdleTimeout: 60 * time.Second,
		ReconnectOnEOF:  true,
	})
	position := ntrip.GGA{Latitude: lat, Longitude: lon, AltitudeMeters: alt, FixQuality: ntrip.FixGPS, Satellites: 12, HDOP: 1.0}
	sentence, err := ntrip.FormatGGA(position)
	if err != nil {
		log.Fatalf("format gga: %v", err)
	}
	if err := stream.SetGGA(sentence); err != nil {
		log.Fatalf("set gga: %v", err)
	}

	writer, err := openSinks(udpAddr, filePath, devicePath, stdout)
	if err != nil {
		log.Fatal(err)
	}
	updatesCtx, stopUpdates := context.WithCancel(ctx)
	updatesDone := make(chan struct{})
	go func() {
		defer close(updatesDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-updatesCtx.Done():
				return
			case <-ticker.C:
				position.Time = time.Now().UTC()
				sentence, err := ntrip.FormatGGA(position)
				if err == nil {
					err = stream.SetGGA(sentence)
				}
				if err != nil {
					log.Printf("update GGA: %v", err)
				}
			}
		}
	}()
	err = stream.RunToWriter(ctx, writer)
	stopUpdates()
	<-updatesDone
	if err != nil && ctx.Err() == nil {
		log.Fatalf("ntrip stream stopped: %v", err)
	}
}

type sinkGroup struct {
	io.Writer
	closers []io.Closer
}

func (s *sinkGroup) Close() error {
	var err error
	for _, closer := range s.closers {
		err = errors.Join(err, closer.Close())
	}
	return err
}

func openSinks(udpAddr, filePath, devicePath string, stdout bool) (_ io.WriteCloser, err error) {
	var sinks []io.Writer
	group := &sinkGroup{}
	defer func() {
		if err != nil {
			err = errors.Join(err, group.Close())
		}
	}()
	if udpAddr != "" {
		conn, err := net.Dial("udp", udpAddr)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, conn)
		group.closers = append(group.closers, conn)
	}
	if filePath != "" {
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, file)
		group.closers = append(group.closers, file)
	}
	if devicePath != "" {
		device, err := os.OpenFile(devicePath, os.O_WRONLY, 0)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, device)
		group.closers = append(group.closers, device)
	}
	if stdout {
		sinks = append(sinks, os.Stdout)
		group.closers = append(group.closers, os.Stdout)
	}
	if len(sinks) == 0 {
		return nil, fmt.Errorf("at least one sink is required: -udp, -file, -device, or -stdout")
	}
	group.Writer = io.MultiWriter(sinks...)
	return group, nil
}
