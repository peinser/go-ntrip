package ntrip

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testGGA = "$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,46.9,M,,*47"

func TestRoverFullDuplexGGA(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	ggaCh := make(chan string, 1)
	headerCh := make(chan map[string]string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		_, headers := readRawRequest(t, reader)
		headerCh <- headers
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nNtrip-Version: Ntrip/2.0\r\n\r\n"))
		_, _ = conn.Write([]byte{0xd3, 0x00, 0x01, 0x42})
		line, _ := reader.ReadString('\n')
		ggaCh <- strings.TrimRight(line, "\r\n")
	}()

	rover, err := DialRover(context.Background(), RoverConfig{
		URL:         "http://" + ln.Addr().String() + "/MOUNT",
		Credentials: Credentials{Username: "rover", Password: "secret"},
	})
	if err != nil {
		t.Fatalf("DialRover() = %v", err)
	}
	defer rover.Close()

	buf := make([]byte, 4)
	if _, err := io.ReadFull(rover, buf); err != nil {
		t.Fatalf("read rtcm = %v", err)
	}
	if string(buf) != string([]byte{0xd3, 0x00, 0x01, 0x42}) {
		t.Fatalf("rtcm = %x", buf)
	}
	if err := rover.WriteGGA(testGGA); err != nil {
		t.Fatalf("WriteGGA() = %v", err)
	}
	if got := <-ggaCh; got != testGGA {
		t.Fatalf("gga = %q", got)
	}
	headers := <-headerCh
	if headers["ntrip-version"] != "Ntrip/2.0" {
		t.Fatalf("ntrip-version header = %q", headers["ntrip-version"])
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("rover:secret"))
	if headers["authorization"] != wantAuth {
		t.Fatalf("authorization = %q", headers["authorization"])
	}
}

func TestSourcePUTChunked(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	methodCh := make(chan string, 1)
	bodyCh := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		requestLine, headers := readRawRequest(t, reader)
		methodCh <- strings.Fields(requestLine)[0]
		if headers["transfer-encoding"] != "chunked" {
			bodyCh <- "missing chunked"
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		sizeLine, _ := reader.ReadString('\n')
		sizeLine = strings.TrimSpace(sizeLine)
		data := make([]byte, 4)
		_, _ = io.ReadFull(reader, data)
		_, _ = reader.ReadString('\n')
		bodyCh <- sizeLine + ":" + string(data)
	}()

	source, err := DialSource(context.Background(), SourceConfig{URL: "http://" + ln.Addr().String() + "/BASE"})
	if err != nil {
		t.Fatalf("DialSource() = %v", err)
	}
	defer source.Close()
	if n, err := source.Write([]byte("RTCM")); err != nil || n != 4 {
		t.Fatalf("source.Write() = %d, %v", n, err)
	}
	if got := <-methodCh; got != "PUT" {
		t.Fatalf("method = %q", got)
	}
	if got := <-bodyCh; got != "4:RTCM" {
		t.Fatalf("chunk body = %q", got)
	}
}

func TestFetchSourcetableTLSAndParse(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("user:pass")) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Ntrip-Version", "Ntrip/2.0")
		_, _ = io.WriteString(w, strings.Join([]string{
			"CAS;caster.example;443;CORSHub;Peinser;1;BEL;50.85;4.35;;;;",
			"NET;OPEN;Peinser;B;N;https://example.invalid;;;;",
			"STR;BASE;Brussels;RTCM 3.3;1005(10),1074(1);2;GPS+GLO;OPEN;BEL;50.85;4.35;1;0;CORSHub;none;B;N;9600;misc",
			"ENDSOURCETABLE",
		}, "\r\n"))
	}))
	defer server.Close()

	st, err := FetchSourcetable(context.Background(), SourcetableConfig{
		URL:         server.URL,
		Credentials: Credentials{Username: "user", Password: "pass"},
		TLSConfig:   &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		t.Fatalf("FetchSourcetable() = %v", err)
	}
	if len(st.Casters) != 1 || len(st.Networks) != 1 || len(st.Streams) != 1 {
		t.Fatalf("unexpected sourcetable sizes: %+v", st)
	}
	stream := st.Streams[0]
	if stream.Mountpoint != "BASE" || !stream.NMEA || stream.Authentication != "B" || stream.Bitrate != 9600 {
		t.Fatalf("stream parse = %+v", stream)
	}
	if st.Header["Ntrip-Version"][0] != "Ntrip/2.0" {
		t.Fatalf("header = %+v", st.Header)
	}
}

func TestParseICYResponse(t *testing.T) {
	resp, err := parseStatusLine("ICY 200 OK")
	if err != nil {
		t.Fatalf("parseStatusLine() = %v", err)
	}
	if resp.Proto != "ICY" || resp.Code != 200 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestFormatAndValidateGGA(t *testing.T) {
	sentence, err := FormatGGA(GGA{
		Time:              time.Date(2026, 6, 20, 12, 34, 56, 0, time.UTC),
		Latitude:          50.85034,
		Longitude:         4.35171,
		FixQuality:        FixGPS,
		Satellites:        12,
		HDOP:              0.8,
		AltitudeMeters:    65.4,
		GeoidHeightMeters: 46.9,
	})
	if err != nil {
		t.Fatalf("FormatGGA() = %v", err)
	}
	if !strings.HasPrefix(sentence, "$GNGGA,123456.00,5051.0204,N,00421.1026,E,1,12,0.8,65.4,M,46.9,M,,") {
		t.Fatalf("sentence = %q", sentence)
	}
	if err := ValidateGGA(sentence); err != nil {
		t.Fatalf("ValidateGGA() = %v", err)
	}
}

func TestSourcetableWarningsLookupAndNearest(t *testing.T) {
	st, err := ParseSourcetable(strings.Join([]string{
		"NET;OPEN;Peinser;B;N;https://example.invalid;https://streams.invalid;mailto:test@example.invalid;misc",
		"STR;NEAR;Near;RTCM 3.3;1005;2;GPS;OPEN;BEL;50.8500;4.3500;1;0;GEN;none;B;N;9600;misc",
		"STR;FAR;Far;RTCM 3.3;1005;2;GPS;OPEN;BEL;51.2000;5.2000;1;0;GEN;none;B;N;9600;misc",
		"STR;BAD;Bad;RTCM 3.3;1005;2;GPS;OPEN;BEL;not-a-lat;4.3500;1;0;GEN;none;B;N;nope;misc",
		"ENDSOURCETABLE",
	}, "\n"))
	if err != nil {
		t.Fatalf("ParseSourcetable() = %v", err)
	}
	if len(st.Warnings) != 2 {
		t.Fatalf("warnings = %+v", st.Warnings)
	}
	if stream, ok := st.HasStream("NEAR"); !ok || stream.Identifier != "Near" {
		t.Fatalf("HasStream() = %+v, %v", stream, ok)
	}
	if network, ok := st.Network("OPEN"); !ok || network.RegistrationAddress == "" {
		t.Fatalf("Network() = %+v, %v", network, ok)
	}
	nearest := st.NearestStreams(50.85, 4.35, 0)
	if len(nearest) != 3 || nearest[0].Stream.Mountpoint != "BAD" && nearest[0].Stream.Mountpoint != "NEAR" {
		t.Fatalf("nearest = %+v", nearest)
	}
	if !strings.Contains(st.String(), "ENDSOURCETABLE\r\n") {
		t.Fatalf("String() = %q", st.String())
	}
}

func TestRoverStreamReconnectsAndResendsGGA(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	connections := make(chan string, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				readRawRequest(t, reader)
				_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
				line, _ := reader.ReadString('\n')
				connections <- strings.TrimRight(line, "\r\n")
				_, _ = conn.Write([]byte("R"))
			}()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream := NewRoverStream(StreamConfig{
		Rover:          RoverConfig{URL: "http://" + ln.Addr().String() + "/ANY"},
		GGAInterval:    10 * time.Millisecond,
		Reconnect:      BackoffConfig{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond},
		ReconnectOnEOF: true,
	})
	if err := stream.SetGGA(testGGA); err != nil {
		t.Fatalf("SetGGA() = %v", err)
	}
	read := 0
	err := stream.Run(ctx, func(p []byte) error {
		read += len(p)
		if read >= 2 {
			cancel()
		}
		return nil
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("stream.Run() = %v", err)
	}
	if got := <-connections; got != testGGA {
		t.Fatalf("first gga = %q", got)
	}
	if got := <-connections; got != testGGA {
		t.Fatalf("second gga = %q", got)
	}
	stats := stream.Stats()
	if stats.Connections < 2 || stats.Reconnects < 1 || stats.BytesRead < 2 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestRoverStreamRejectsInvalidGGA(t *testing.T) {
	stream := NewRoverStream(StreamConfig{})
	if err := stream.SetGGA("$GPGGA,bad*00"); err == nil {
		t.Fatal("expected invalid GGA error")
	}
}

func TestRoverHeaderTimeout(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		time.Sleep(200 * time.Millisecond)
	}()

	_, err := DialRover(context.Background(), RoverConfig{
		URL:           "http://" + ln.Addr().String() + "/MOUNT",
		HeaderTimeout: 20 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected header timeout")
	}
}

func TestRoverStreamReadIdleReconnects(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	accepted := make(chan struct{}, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(index int) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				readRawRequest(t, reader)
				_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
				accepted <- struct{}{}
				if index == 1 {
					_, _ = conn.Write([]byte("R"))
				}
				time.Sleep(200 * time.Millisecond)
			}(i)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream := NewRoverStream(StreamConfig{
		Rover:           RoverConfig{URL: "http://" + ln.Addr().String() + "/ANY"},
		ReadIdleTimeout: 20 * time.Millisecond,
		Reconnect:       BackoffConfig{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond},
		ReconnectOnEOF:  true,
	})
	read := 0
	err := stream.Run(ctx, func(p []byte) error {
		read += len(p)
		cancel()
		return nil
	})
	if err != nil && err != context.Canceled {
		t.Fatalf("stream.Run() = %v", err)
	}
	<-accepted
	<-accepted
	if read != 1 {
		t.Fatalf("read = %d", read)
	}
	if stats := stream.Stats(); stats.Reconnects < 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestRoverStreamStopsOnUnauthorized(t *testing.T) {
	ln := listenTCP(t)
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		readRawRequest(t, reader)
		_, _ = conn.Write([]byte("HTTP/1.1 401 Unauthorized\r\n\r\n"))
	}()
	stream := NewRoverStream(StreamConfig{
		Rover:     RoverConfig{URL: "http://" + ln.Addr().String() + "/ANY"},
		Reconnect: BackoffConfig{Min: time.Millisecond, Max: time.Millisecond},
	})
	err := stream.Run(context.Background(), func([]byte) error { return nil })
	var status *StatusError
	if !errors.As(err, &status) || status.Code != 401 {
		t.Fatalf("err = %v", err)
	}
}

func listenTCP(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	return ln
}

func readRawRequest(t *testing.T, reader *bufio.Reader) (string, map[string]string) {
	t.Helper()
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read request line: %v", err)
	}
	headers := make(map[string]string)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return strings.TrimRight(requestLine, "\r\n"), headers
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("bad header line: %q", line)
		}
		headers[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
}
