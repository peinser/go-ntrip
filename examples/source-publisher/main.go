package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	ntrip "github.com/peinser/go-ntrip"
)

func main() {
	var url, username, password, filePath string
	flag.StringVar(&url, "url", "", "NTRIP source URL, for example https://caster.example/BASE-01")
	flag.StringVar(&username, "username", "", "NTRIP username")
	flag.StringVar(&password, "password", "", "NTRIP password")
	flag.StringVar(&filePath, "file", "", "optional RTCM input file; stdin is used when empty")
	flag.Parse()
	if url == "" {
		log.Fatal("-url is required")
	}

	input, err := openInput(filePath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	stream := ntrip.NewSourceStream(ntrip.SourceStreamConfig{
		Source: ntrip.SourceConfig{
			URL:         url,
			Credentials: ntrip.Credentials{Username: username, Password: password},
		},
		Reconnect: ntrip.BackoffConfig{Min: time.Second, Max: 30 * time.Second, Factor: 2},
	})
	log.Printf("publishing RTCM to %s", url)
	if err := stream.Run(ctx, input); err != nil && ctx.Err() == nil {
		log.Fatalf("source stream stopped: %v", err)
	}
}

func openInput(path string) (*os.File, error) {
	if path == "" {
		return os.Stdin, nil
	}
	return os.Open(path)
}
