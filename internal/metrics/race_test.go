package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/terjesannum/tibber-exporter/internal/home"
)

type wsMsg struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// fakeFeed acks, then sends a liveMeasurement every 2 ms for duration (0
// means until the client goes away), then goes silent.
func fakeFeed(t *testing.T, duration time.Duration) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var m wsMsg
		_ = wsjson.Read(ctx, c, &m)
		_ = wsjson.Write(ctx, c, wsMsg{Type: "connection_ack"})
		_ = wsjson.Read(ctx, c, &m)
		go func() {
			defer cancel()
			for {
				if _, _, err := c.Read(ctx); err != nil {
					return
				}
			}
		}()
		end := time.Now().Add(duration)
		for i := 0; duration == 0 || time.Now().Before(end); i++ {
			p := fmt.Sprintf(`{"data":{"liveMeasurement":{"timestamp":%q,"power":%d,"accumulatedConsumption":%d,"currentL1":1.5,"accumulatedReward":0.1}}}`,
				time.Now().Format(time.RFC3339Nano), i, i)
			if err := wsjson.Write(ctx, c, wsMsg{ID: m.ID, Type: "next", Payload: json.RawMessage(p)}); err != nil {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		<-ctx.Done()
	}))
	t.Cleanup(srv.Close)
	return strings.Replace(srv.URL, "http", "ws", 1)
}

// startFeed runs the live feed as main.go does and returns a function that
// stops it and waits until it has returned.
func startFeed(t *testing.T, h *home.Home, url string, readTimeout time.Duration) func() {
	log.SetOutput(io.Discard)
	id := string(h.Id)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		h.RunLiveFeed(ctx, home.LiveFeed{
			HTTPClient:  http.DefaultClient,
			Url:         url,
			Token:       "dummy",
			ReadTimeout: readTimeout,
			OnEnd: func(reason string) {
				LiveReconnects.WithLabelValues(id, reason).Inc()
			},
		})
	})
	stop := func() {
		cancel()
		wg.Wait()
		log.SetOutput(os.Stderr)
	}
	t.Cleanup(stop)
	return stop
}

// value returns the value of the series with the given name and labels.
func value(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metrics:
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want != l.GetValue() {
					continue metrics
				}
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue(), true
			}
			return m.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The live feed writes while /metrics reads, as in production. Run with -race.
func TestLiveFeedRace(t *testing.T) {
	h := home.New("race-home")
	stop := startFeed(t, h, fakeFeed(t, 0), time.Minute)
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewMeasurementCollector("race-home", h))
	reg.MustRegister(NewHomeCollector(h))
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if _, err := reg.Gather(); err != nil {
			t.Fatal(err)
		}
		h.LiveStatus()
		time.Sleep(time.Millisecond)
	}
	labels := map[string]string{"home_id": "race-home"}
	if v, _ := value(t, reg, "tibber_live_connected", labels); v != 1 {
		t.Errorf("tibber_live_connected = %v, want 1", v)
	}
	if v, ok := value(t, reg, "tibber_power_consumption", labels); !ok || v == 0 {
		t.Errorf("tibber_power_consumption = %v (present: %v), want the streamed value", v, ok)
	}
	if v, _ := value(t, reg, "tibber_live_last_message_timestamp_seconds", labels); time.Since(time.Unix(int64(v), 0)) > 5*time.Second {
		t.Errorf("tibber_live_last_message_timestamp_seconds = %v, want about now", v)
	}
	stop()
}

// A feed that goes silent shows as disconnected, with the reconnect reason,
// while the last message time stays where it was. Alerts can use both.
func TestDeadFeedIsVisible(t *testing.T) {
	h := home.New("dead-home")
	startFeed(t, h, fakeFeed(t, 50*time.Millisecond), 300*time.Millisecond)
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewMeasurementCollector("dead-home", h))
	reg.MustRegister(LiveReconnects)
	labels := map[string]string{"home_id": "dead-home"}
	waitFor(t, "data", func() bool {
		v, _ := value(t, reg, "tibber_live_connected", labels)
		return v == 1
	})
	waitFor(t, "a read timeout", func() bool {
		v, _ := value(t, reg, "tibber_live_reconnects_total", map[string]string{"home_id": "dead-home", "reason": home.EndReadTimeout})
		return v >= 1
	})
	waitFor(t, "the feed to show as disconnected", func() bool {
		v, _ := value(t, reg, "tibber_live_connected", labels)
		return v == 0
	})
	last, ok := value(t, reg, "tibber_live_last_message_timestamp_seconds", labels)
	if !ok || time.Since(time.Unix(0, int64(last*1e9))) < 250*time.Millisecond {
		t.Errorf("tibber_live_last_message_timestamp_seconds = %v (present: %v), want the time of the last message", last, ok)
	}
}

// The scrape and the probes collect several times a minute. A feed that has
// no data is logged once, not on every collect.
func TestStaleMeasurementsLoggedOnce(t *testing.T) {
	var logs strings.Builder
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewMeasurementCollector("stale-home", home.New("stale-home")))
	for range 3 {
		if _, err := reg.Gather(); err != nil {
			t.Fatal(err)
		}
	}
	if n := strings.Count(logs.String(), "Measurements too old"); n != 1 {
		t.Errorf("%d \"Measurements too old\" lines for 3 collects, want 1", n)
	}
}
