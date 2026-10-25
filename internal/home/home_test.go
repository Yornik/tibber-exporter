package home

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/hasura/go-graphql-client"
	"github.com/terjesannum/tibber-exporter/internal/tibber"
)

type wsMsg struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// syncBuffer is safe for the library's goroutines, which log after Run returns.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSubscriberDoesNotLogToken(t *testing.T) {
	const token = "test-token-must-not-be-logged"
	var logs syncBuffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)

	sentToken := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		var m wsMsg
		_ = wsjson.Read(ctx, c, &m) // connection_init
		var params struct{ Token string }
		_ = json.Unmarshal(m.Payload, &params)
		sentToken <- params.Token
		_ = wsjson.Write(ctx, c, wsMsg{Type: "connection_ack"})
		_ = wsjson.Read(ctx, c, &m) // subscribe
		_ = wsjson.Write(ctx, c, wsMsg{ID: m.ID, Type: "next", Payload: json.RawMessage(`{"data":{"liveMeasurement":{"power":42}}}`)})
		_ = wsjson.Write(ctx, c, wsMsg{ID: m.ID, Type: "complete"})
		<-ctx.Done()
	}))
	defer srv.Close()

	sc := newSubscriber(strings.Replace(srv.URL, "http", "ws", 1), http.DefaultClient, token)
	var m Measurements
	if _, err := sc.Subscribe(&m, map[string]interface{}{"id": graphql.ID("home")}, func([]byte, error) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Run returns when the server completes the only subscription.
	_ = sc.RunWithContext(ctx)
	sc.Close()

	if got := <-sentToken; got != token {
		t.Fatalf("the server got token %q, want the configured token", got)
	}
	if strings.Contains(logs.String(), token) {
		t.Fatal("the token is in the log")
	}
	// The other protocol messages are still logged, except each measurement.
	if !strings.Contains(logs.String(), `"type":"subscribe"`) {
		t.Fatalf("the subscribe message is not in the log:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), `"type":"next"`) {
		t.Fatalf("a measurement is in the log:\n%s", logs.String())
	}
}

func TestLatestPeriodCoversDSTDays(t *testing.T) {
	ams, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Skip("no time zone data:", err)
	}
	// The node for 2026-10-24 ends at local midnight; 2026-10-25 has 25 hours.
	end := time.Date(2026, 10, 25, 0, 0, 0, 0, ams)
	cases := []struct {
		name string
		res  tibber.EnergyResolution
		to   time.Time
		now  time.Time
		want bool
	}{
		{"25-hour day, last hour", tibber.ResolutionDaily, end, time.Date(2026, 10, 25, 23, 30, 0, 0, ams), true},
		{"25-hour day, next day", tibber.ResolutionDaily, end, time.Date(2026, 10, 26, 0, 30, 0, 0, ams), false},
		{"23-hour day, last hour", tibber.ResolutionDaily, time.Date(2026, 3, 29, 0, 0, 0, 0, ams), time.Date(2026, 3, 29, 23, 30, 0, 0, ams), true},
		{"normal day, next morning", tibber.ResolutionDaily, time.Date(2026, 10, 23, 0, 0, 0, 0, ams), time.Date(2026, 10, 24, 1, 30, 0, 0, ams), false},
		{"hour, within", tibber.ResolutionHourly, end, end.Add(59 * time.Minute), true},
		{"hour, next", tibber.ResolutionHourly, end, end.Add(61 * time.Minute), false},
	}
	for _, c := range cases {
		if got := latestPeriod(c.res, c.to, c.now); got != c.want {
			t.Errorf("%s: latestPeriod = %v, want %v", c.name, got, c.want)
		}
	}
}
