package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/hasura/go-graphql-client"
	"github.com/terjesannum/tibber-exporter/internal/backoff"
)

const homesBody = `{"data":{"viewer":{"websocketSubscriptionUrl":"wss://websocket-api.tibber.com/v1-beta/gql/subscriptions","homes":[{"id":"home-1","appNickname":"Home"}]}}}`

func testBackoff() *backoff.Backoff {
	return &backoff.Backoff{Base: 10 * time.Millisecond, Max: 20 * time.Millisecond}
}

// Tibber answered the homes query with 504 and 502 at start-up; the exporter
// must retry instead of exiting.
func TestQueryHomesRetries(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			http.Error(w, "Gateway Timeout", http.StatusGatewayTimeout)
		case 2:
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(homesBody))
		}
	}))
	defer srv.Close()

	hq, err := queryHomes(context.Background(), graphql.NewClient(srv.URL, nil), testBackoff())
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Errorf("%d queries, want 3", calls.Load())
	}
	if len(hq.Viewer.Homes) != 1 || hq.Viewer.Homes[0].Id != "home-1" || hq.Viewer.WebsocketSubscriptionUrl == "" {
		t.Errorf("homes = %+v", hq.Viewer)
	}
}

func TestQueryHomesStopsOnShutdown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := queryHomes(ctx, graphql.NewClient(srv.URL, nil), testBackoff()); err == nil {
		t.Fatal("queryHomes succeeded against a failing API")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("queryHomes returned %s after shutdown", d)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransportSendsTokenOnlyToTibber(t *testing.T) {
	got := map[string]string{}
	base := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got[r.URL.Host] = r.Header.Get("Authorization")
		if r.Header.Get("User-Agent") != "test-agent" {
			t.Errorf("%s: User-Agent %q", r.URL.Host, r.Header.Get("User-Agent"))
		}
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: http.NoBody, Request: r}
		if r.URL.Host == "api.tibber.com" {
			// A redirect to another host must not carry the token there.
			resp.StatusCode = http.StatusFound
			resp.Header.Set("Location", "https://other.example.com/")
		}
		return resp, nil
	})
	client := &http.Client{Transport: &transport{Token: "secret", UserAgent: "test-agent", base: base}}

	req, _ := http.NewRequest(http.MethodGet, "https://api.tibber.com/v1-beta/gql", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, err := client.Get("https://websocket-api.tibber.com/v1-beta/gql/subscriptions"); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"api.tibber.com":           "Bearer secret",
		"websocket-api.tibber.com": "Bearer secret",
		"other.example.com":        "",
	}
	for host, auth := range want {
		if a, ok := got[host]; !ok || a != auth {
			t.Errorf("%s got Authorization %q (requested: %v), want %q", host, a, ok, auth)
		}
	}
	if len(req.Header) != 0 {
		t.Errorf("the caller's request was changed: %v", req.Header)
	}
}

// The websocket handshake still works through the transport, which now
// sends a copy of the request.
func TestTransportWebsocketHandshake(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("the token was sent to a host that is not Tibber")
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		c.Close(websocket.StatusNormalClosure, "")
	}))
	defer srv.Close()
	hc := &http.Client{Timeout: 5 * time.Second, Transport: &transport{Token: "secret", UserAgent: "test-agent"}}
	c, _, err := websocket.Dial(context.Background(), strings.Replace(srv.URL, "http", "ws", 1), &websocket.DialOptions{HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	c.CloseNow()
}
