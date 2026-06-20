package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/url"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ntrip "github.com/peinser/go-ntrip"
)

func main() {
	var casterURL, username, password, udpAddr string
	var lat, lon, maxDistance float64
	flag.StringVar(&casterURL, "caster", "", "caster base URL, for example https://corshub.peinser.com")
	flag.StringVar(&username, "username", "", "NTRIP username")
	flag.StringVar(&password, "password", "", "NTRIP password")
	flag.StringVar(&udpAddr, "udp", "127.0.0.1:13320", "UDP address that receives raw RTCM bytes")
	flag.Float64Var(&lat, "lat", 0, "rover latitude in degrees")
	flag.Float64Var(&lon, "lon", 0, "rover longitude in degrees")
	flag.Float64Var(&maxDistance, "max-distance", 50_000, "maximum mountpoint distance in meters; <=0 disables filtering")
	flag.Parse()
	if casterURL == "" {
		log.Fatal("-caster is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	table, err := ntrip.FetchSourcetable(ctx, ntrip.SourcetableConfig{
		URL:         casterURL,
		Credentials: ntrip.Credentials{Username: username, Password: password},
	})
	if err != nil {
		log.Fatalf("fetch sourcetable: %v", err)
	}
	nearest := table.NearestStreams(lat, lon, maxDistance)
	if len(nearest) == 0 {
		log.Fatal("no suitable mountpoints found")
	}
	mountpoint := nearest[0].Stream.Mountpoint
	streamURL, err := joinMountpoint(casterURL, mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("selected mountpoint %s at %.0f m", mountpoint, nearest[0].Meters)

	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		log.Fatalf("dial udp sink: %v", err)
	}
	defer conn.Close()

	stream := ntrip.NewRoverStream(ntrip.StreamConfig{
		Rover:           ntrip.RoverConfig{URL: streamURL, Credentials: ntrip.Credentials{Username: username, Password: password}},
		ReadIdleTimeout: 60 * time.Second,
		ReconnectOnEOF:  true,
	})
	sentence, err := ntrip.FormatGGA(ntrip.GGA{Time: time.Now().UTC(), Latitude: lat, Longitude: lon, FixQuality: ntrip.FixGPS, Satellites: 12, HDOP: 1.0})
	if err != nil {
		log.Fatalf("format gga: %v", err)
	}
	if err := stream.SetGGA(sentence); err != nil {
		log.Fatalf("set gga: %v", err)
	}
	if err := stream.RunToWriter(ctx, conn); err != nil && ctx.Err() == nil {
		log.Fatalf("ntrip stream stopped: %v", err)
	}
}

func joinMountpoint(base, mountpoint string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(mountpoint, "/")
	return u.String(), nil
}
