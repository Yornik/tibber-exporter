package home

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/terjesannum/tibber-exporter/internal/backoff"
)

const testToken = "test-token-must-not-be-logged"

// feedConn is one accepted connection of the fake feed, after connection_init
// and subscribe.
type feedConn struct {
	c   *websocket.Conn
	ctx context.Context // ends when the client goes away
	id  string          // subscription id
	raw net.Conn
}

func (fc *feedConn) send(typ, payload string) error {
	m := wsMsg{ID: fc.id, Type: typ}
	if payload != "" {
		m.Payload = json.RawMessage(payload)
	}
	return wsjson.Write(fc.ctx, fc.c, m)
}

func (fc *feedConn) data(ts time.Time, power int) error {
	return fc.send("next", fmt.Sprintf(`{"data":{"liveMeasurement":{"timestamp":%q,"power":%d}}}`, ts.Format(time.RFC3339Nano), power))
}

// stream sends a measurement every 10 ms until the client goes away.
func stream(power int) func(*feedConn) {
	return func(fc *feedConn) {
		for fc.data(time.Now(), power) == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// fakeFeed is a fake Tibber graphql-transport-ws server. Connection n runs
// behaviours[n-1], and the last one repeats. A nil behaviour answers the
// websocket handshake with 502 Bad Gateway.
type fakeFeed struct {
	*httptest.Server
	conns    atomic.Int32
	open     atomic.Int32
	overlap  atomic.Bool // a connection was opened while another was open
	mu       sync.Mutex
	started  []time.Time            // when each connection arrived
	ended    []time.Time            // when each handler returned
	closes   []websocket.StatusCode // close code sent by the client, -1 for none
	complete atomic.Int32           // complete messages sent by the client
	hijacked net.Conn
	// readDelay holds back the reads after subscribe, so a close frame from
	// the client is answered late, as by a slow or stuck peer.
	readDelay atomic.Int64
}

func newFakeFeed(t *testing.T, behaviours ...func(*feedConn)) *fakeFeed {
	f := &fakeFeed{}
	f.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(f.conns.Add(1))
		f.mu.Lock()
		f.started = append(f.started, time.Now())
		f.mu.Unlock()
		defer func() {
			f.mu.Lock()
			f.ended = append(f.ended, time.Now())
			f.mu.Unlock()
		}()
		b := behaviours[min(n, len(behaviours))-1]
		if b == nil {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
		if err != nil {
			t.Error(err)
			return
		}
		if f.open.Add(1) > 1 {
			f.overlap.Store(true)
		}
		defer f.open.Add(-1)
		defer c.CloseNow()
		f.mu.Lock()
		raw := f.hijacked
		f.mu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var m wsMsg
		if err := wsjson.Read(ctx, c, &m); err != nil {
			return
		}
		if m.Type != "connection_init" {
			t.Errorf("connection %d: got %s, want connection_init", n, m.Type)
			return
		}
		var params struct{ Token string }
		_ = json.Unmarshal(m.Payload, &params)
		if params.Token != testToken {
			t.Errorf("connection %d: the server got token %q, want the configured token", n, params.Token)
		}
		_ = wsjson.Write(ctx, c, wsMsg{Type: "connection_ack"})
		if err := wsjson.Read(ctx, c, &m); err != nil {
			return
		}
		if m.Type != "subscribe" {
			t.Errorf("connection %d: got %s, want subscribe", n, m.Type)
			return
		}
		// Keep reading, so that close frames are answered and ctx ends
		// when the client goes away.
		go func() {
			defer cancel()
			time.Sleep(time.Duration(f.readDelay.Load()))
			for {
				_, data, err := c.Read(ctx)
				if err != nil {
					f.mu.Lock()
					f.closes = append(f.closes, websocket.CloseStatus(err))
					f.mu.Unlock()
					return
				}
				if strings.Contains(string(data), `"type":"complete"`) {
					f.complete.Add(1)
				}
			}
		}()
		b(&feedConn{c: c, ctx: ctx, id: m.ID, raw: raw})
	}))
	f.Config.ConnState = func(c net.Conn, s http.ConnState) {
		if s == http.StateHijacked {
			f.mu.Lock()
			f.hijacked = c
			f.mu.Unlock()
		}
	}
	f.Start()
	t.Cleanup(f.Close)
	return f
}

func (f *fakeFeed) wsUrl() string {
	return strings.Replace(f.URL, "http", "ws", 1)
}

func captureLog(t *testing.T) *syncBuffer {
	var logs syncBuffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &logs
}

// runFeed starts RunLiveFeed and returns the reasons seen so far and a stop
// function that waits until RunLiveFeed has returned.
func runFeed(t *testing.T, h *Home, f LiveFeed) (reasons func() []string, stop func()) {
	var mu sync.Mutex
	var got []string
	f.OnEnd = func(r string) {
		mu.Lock()
		got = append(got, r)
		mu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.RunLiveFeed(ctx, f)
	}()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("RunLiveFeed did not return after cancel")
		}
	}
	t.Cleanup(stop)
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}, stop
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

// Every way a session ended in production, in one run: each one is followed
// by a reconnect in the same process, with one connection per attempt.
func TestRunLiveFeedSurvivesEveryEndReason(t *testing.T) {
	logs := captureLog(t)
	feed := newFakeFeed(t,
		// Silence, as at Tibber's 24 h cut: the read timeout fires.
		func(fc *feedConn) {
			_ = fc.data(time.Now(), 1)
			<-fc.ctx.Done()
		},
		// "unable to start stream" (10-25 incident): error, then complete.
		func(fc *feedConn) {
			_ = fc.send("next", `{"errors":[{"message":"unable to start stream for device"}]}`)
			_ = fc.send("complete", "")
			<-fc.ctx.Done()
		},
		func(fc *feedConn) {
			_ = fc.data(time.Now(), 3)
			_ = fc.c.Close(websocket.StatusGoingAway, "Server restart")
		},
		// An error that the stream outlived does not decide the reason.
		func(fc *feedConn) {
			_ = fc.data(time.Now(), 3)
			_ = fc.send("next", `{"errors":[{"message":"temporary"}]}`)
			_ = fc.data(time.Now(), 3)
			_ = fc.c.Close(websocket.StatusGoingAway, "Server restart")
		},
		// EOF: the TCP connection closes without a close frame.
		func(fc *feedConn) {
			_ = fc.data(time.Now(), 4)
			_ = fc.c.CloseNow()
		},
		// Connection reset by peer.
		func(fc *feedConn) {
			_ = fc.data(time.Now(), 5)
			tc := fc.raw.(*net.TCPConn)
			_ = tc.SetLinger(0)
			_ = tc.Close()
		},
		nil, // dial failure: 502 on the websocket handshake
		func(fc *feedConn) {
			_ = fc.send("complete", "")
			<-fc.ctx.Done()
		},
		stream(100),
	)
	h := New("home")
	reasons, stop := runFeed(t, h, LiveFeed{
		HTTPClient:  http.DefaultClient,
		Url:         feed.wsUrl(),
		Token:       testToken,
		ReadTimeout: 300 * time.Millisecond,
		backoff:     &backoff.Backoff{Base: 10 * time.Millisecond, Max: 40 * time.Millisecond},
	})
	waitFor(t, "the feed to recover", func() bool {
		connected, _ := h.LiveStatus()
		lm, _, _ := h.LiveSnapshot()
		return connected && lm.Power == 100
	})
	stop()

	want := []string{EndReadTimeout, EndGraphQLError, EndServerClose, EndServerClose, EndConnectionLost, EndConnectionLost, EndDialFailed, EndComplete}
	if got := reasons(); !slices.Equal(got, want) {
		t.Errorf("reasons = %v, want %v", got, want)
	}
	if n := feed.conns.Load(); n != int32(len(want)+1) {
		t.Errorf("%d connections for %d attempts", n, len(want)+1)
	}
	if connected, _ := h.LiveStatus(); connected {
		t.Error("connected after the feed stopped")
	}
	if !strings.Contains(logs.String(), "Subscription error for home home") || !strings.Contains(logs.String(), "unable to start stream") {
		t.Error("Tibber's error text is not in the log")
	}
	// The cause of a failed dial is in the log, not only the reason.
	if !strings.Contains(logs.String(), "ended: dial_failed (") || !strings.Contains(logs.String(), "got 502") {
		t.Error("the dial error is not in the log")
	}
	if strings.Contains(logs.String(), testToken) {
		t.Error("the token is in the log")
	}
}

// The exporter closes a session before Tibber's 24 h cut and only then opens
// the next one, so it never holds two connections for one token.
func TestRunLiveFeedReconnectsBeforeMaxAge(t *testing.T) {
	captureLog(t)
	feed := newFakeFeed(t, stream(1))
	h := New("home")
	reasons, stop := runFeed(t, h, LiveFeed{
		HTTPClient:  http.DefaultClient,
		Url:         feed.wsUrl(),
		Token:       testToken,
		ReadTimeout: time.Minute,
		maxAge:      150 * time.Millisecond,
		backoff:     &backoff.Backoff{Base: 50 * time.Millisecond, Max: 50 * time.Millisecond},
	})
	waitFor(t, "two sessions", func() bool { return len(reasons()) >= 2 })
	stop()
	for _, r := range reasons() {
		if r != EndMaxAge {
			t.Errorf("reason %q, want %q", r, EndMaxAge)
		}
	}
	if feed.overlap.Load() {
		t.Error("a new connection was opened before the old one was closed")
	}
	// Max age and shutdown both end the subscription and close with a close frame.
	waitFor(t, "the server to see every close", func() bool {
		feed.mu.Lock()
		defer feed.mu.Unlock()
		return len(feed.closes) == int(feed.conns.Load())
	})
	feed.mu.Lock()
	defer feed.mu.Unlock()
	for i, code := range feed.closes {
		if code != websocket.StatusNormalClosure {
			t.Errorf("connection %d closed with %v, want a normal close frame", i+1, code)
		}
	}
	if n := feed.complete.Load(); n != feed.conns.Load() {
		t.Errorf("%d complete messages for %d connections", n, feed.conns.Load())
	}
}

// The delay doubles while attempts fail, and starts from the base again after
// a session that delivered data for long enough.
func TestRunLiveFeedBackoff(t *testing.T) {
	captureLog(t)
	healthy := func(fc *feedConn) {
		for end := time.Now().Add(150 * time.Millisecond); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			_ = fc.data(time.Now(), 2)
		}
		_ = fc.c.CloseNow()
	}
	feed := newFakeFeed(t, nil, nil, nil, nil, nil, nil, nil, healthy, stream(3))
	h := New("home")
	_, stop := runFeed(t, h, LiveFeed{
		HTTPClient:  http.DefaultClient,
		Url:         feed.wsUrl(),
		Token:       testToken,
		ReadTimeout: time.Minute,
		healthy:     100 * time.Millisecond,
		backoff:     &backoff.Backoff{Base: 10 * time.Millisecond, Max: 10 * time.Second},
	})
	waitFor(t, "data after the healthy session", func() bool {
		lm, _, _ := h.LiveSnapshot()
		return lm.Power == 3
	})
	stop()

	feed.mu.Lock()
	defer feed.mu.Unlock()
	// Seven failed attempts: steps 10, 20, ... 640 ms, each delay at least half the step.
	for i := 1; i < 8; i++ {
		gap := feed.started[i].Sub(feed.ended[i-1])
		if want := 10 * time.Millisecond << (i - 1) / 2; gap < want {
			t.Errorf("delay before attempt %d = %s, want at least %s", i+1, gap, want)
		}
	}
	// Without the reset the next delay would be at least 640 ms.
	if gap := feed.started[8].Sub(feed.ended[7]); gap > 300*time.Millisecond {
		t.Errorf("delay after a healthy session = %s, want about 10 ms", gap)
	}
}

func TestApplyMeasurementOrder(t *testing.T) {
	captureLog(t)
	h := New("home")
	now := time.Now()
	received := now
	apply := func(ts time.Time, power float64, wantNew bool) {
		t.Helper()
		var m Measurements
		m.LiveMeasurement.Timestamp = ts
		m.LiveMeasurement.Power = power
		signal := power
		m.LiveMeasurement.SignalStrength = &signal
		if got := h.applyMeasurement(m, received); got != wantNew {
			t.Fatalf("measurement %s %v: new = %v, want %v", ts, power, got, wantNew)
		}
	}
	check := func(ts time.Time, power float64) {
		t.Helper()
		lm, tv, _ := h.LiveSnapshot()
		if !lm.Timestamp.Equal(ts) || lm.Power != power || !tv.SignalStrength.Timestamp.Equal(ts) || tv.SignalStrength.Value != power {
			t.Fatalf("stored %s %v (signal %s %v), want %s %v", lm.Timestamp, lm.Power, tv.SignalStrength.Timestamp, tv.SignalStrength.Value, ts, power)
		}
	}
	t1 := now.Add(-20 * time.Second)
	t2 := now.Add(-10 * time.Second)
	apply(t2, 2, true)
	check(t2, 2)
	apply(t1, 1, false) // older: skipped
	check(t2, 2)
	apply(t2, 3, false) // same timestamp: kept, but not new
	check(t2, 3)
	apply(now, 4, true)
	check(now, 4)
	// A repeated measurement does not count as a received one, so a feed
	// that only repeats itself shows its real age.
	received = now.Add(time.Second)
	apply(now, 4, false)
	if _, last := h.LiveStatus(); !last.Equal(now) {
		t.Errorf("last message %s, want the receive time %s", last, now)
	}

	// A timestamp far in the future must not freeze the feed.
	future := now.Add(time.Hour)
	apply(future, 5, true)
	apply(now.Add(time.Second), 6, true)
	check(now.Add(time.Second), 6)
}

// countingClient counts the client's open TCP connections and records how
// many were open at each dial.
type countingClient struct {
	*http.Client
	mu     sync.Mutex
	open   int
	atDial []int
}

func newCountingClient() *countingClient {
	cc := &countingClient{}
	var d net.Dialer
	cc.Client = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			cc.mu.Lock()
			cc.atDial = append(cc.atDial, cc.open)
			cc.mu.Unlock()
			c, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			cc.mu.Lock()
			cc.open++
			cc.mu.Unlock()
			return &countedConn{Conn: c, cc: cc}, nil
		},
	}}
	return cc
}

type countedConn struct {
	net.Conn
	cc   *countingClient
	once sync.Once
}

func (c *countedConn) Close() error {
	c.once.Do(func() {
		c.cc.mu.Lock()
		c.cc.open--
		c.cc.mu.Unlock()
	})
	return c.Conn.Close()
}

// After Tibber's error and complete, the library closes the socket on its own
// goroutine. The next attempt must still wait until that socket is closed, also
// when the peer is slow to answer the close frame.
func TestRunLiveFeedClosesBeforeRedial(t *testing.T) {
	captureLog(t)
	errorComplete := func(fc *feedConn) {
		_ = fc.send("next", `{"errors":[{"message":"unable to start stream for device"}]}`)
		_ = fc.send("complete", "")
		<-fc.ctx.Done()
	}
	feed := newFakeFeed(t, errorComplete, errorComplete, stream(1))
	feed.readDelay.Store(int64(300 * time.Millisecond))
	hc := newCountingClient()
	h := New("home")
	reasons, stop := runFeed(t, h, LiveFeed{
		HTTPClient:  hc.Client,
		Url:         feed.wsUrl(),
		Token:       testToken,
		ReadTimeout: time.Minute,
		backoff:     &backoff.Backoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond},
	})
	waitFor(t, "the third session", func() bool {
		lm, _, _ := h.LiveSnapshot()
		return lm.Power == 1
	})
	stop()
	if got, want := reasons(), []string{EndGraphQLError, EndGraphQLError}; !slices.Equal(got, want) {
		t.Errorf("reasons = %v, want %v", got, want)
	}
	hc.mu.Lock()
	defer hc.mu.Unlock()
	for i, n := range hc.atDial {
		if n != 0 {
			t.Errorf("dial %d: %d connection(s) still open", i+1, n)
		}
	}
}

// Frames that carry no new measurement keep the socket busy, so the read
// timeout never fires. The session must still end after the read timeout.
func TestRunLiveFeedEndsSessionWithoutNewData(t *testing.T) {
	captureLog(t)
	every20ms := func(send func(fc *feedConn) error) func(*feedConn) {
		return func(fc *feedConn) {
			for send(fc) == nil {
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	stale := time.Now()
	feed := newFakeFeed(t,
		// graphql-ws keep-alives only.
		every20ms(func(fc *feedConn) error { return wsjson.Write(fc.ctx, fc.c, wsMsg{Type: "ping"}) }),
		// Errors without complete.
		every20ms(func(fc *feedConn) error {
			return fc.send("next", `{"errors":[{"message":"device offline"}]}`)
		}),
		// The same measurement again and again.
		every20ms(func(fc *feedConn) error { return fc.data(stale, 3) }),
		stream(100),
	)
	h := New("home")
	reasons, stop := runFeed(t, h, LiveFeed{
		HTTPClient:  http.DefaultClient,
		Url:         feed.wsUrl(),
		Token:       testToken,
		ReadTimeout: 300 * time.Millisecond,
		backoff:     &backoff.Backoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond},
	})
	waitFor(t, "the feed to recover", func() bool {
		lm, _, _ := h.LiveSnapshot()
		return lm.Power == 100
	})
	stop()
	want := []string{EndReadTimeout, EndGraphQLError, EndReadTimeout}
	if got := reasons(); !slices.Equal(got, want) {
		t.Errorf("reasons = %v, want %v", got, want)
	}
}
