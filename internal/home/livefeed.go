package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/hasura/go-graphql-client"
	"github.com/terjesannum/tibber-exporter/internal/backoff"
)

// Why a live feed session ended: the reason label of tibber_live_reconnects_total.
const (
	EndReadTimeout    = "read_timeout"    // no new measurement for the read timeout, also Tibber's silent cut at 24 h
	EndGraphQLError   = "graphql_error"   // Tibber sent an error ("unable to start stream ...") and no data after it
	EndComplete       = "complete"        // Tibber completed the subscription without an error
	EndServerClose    = "server_close"    // a close frame, e.g. 1001 "Server restart"
	EndConnectionLost = "connection_lost" // EOF or connection reset
	EndDialFailed     = "dial_failed"     // connect, TLS or websocket handshake failed
	EndMaxAge         = "max_age"         // closed by the exporter before Tibber's 24 h cut
)

var EndReasons = []string{EndReadTimeout, EndGraphQLError, EndComplete, EndServerClose, EndConnectionLost, EndDialFailed, EndMaxAge}

// LiveFeed configures RunLiveFeed.
type LiveFeed struct {
	HTTPClient *http.Client
	Url        string
	Token      string
	// A session that delivers no new measurement for ReadTimeout (default 1
	// minute) is closed.
	ReadTimeout time.Duration
	// OnEnd is called with the reason when a session ends, except on shutdown.
	OnEnd func(reason string)

	// Zero means the production value. The tests shorten them.
	maxAge  time.Duration
	healthy time.Duration
	backoff *backoff.Backoff
}

func (f *LiveFeed) setDefaults() {
	if f.ReadTimeout == 0 {
		f.ReadTimeout = time.Minute
	}
	if f.maxAge == 0 {
		// Tibber goes silent when a connection is 24 h old. Reconnect just before.
		f.maxAge = 23*time.Hour + 55*time.Minute
	}
	if f.healthy == 0 {
		// A session that delivered data for this long resets the backoff.
		f.healthy = 10 * time.Minute
	}
	if f.backoff == nil {
		f.backoff = backoff.New()
	}
}

// RunLiveFeed keeps the live subscription open until ctx ends. Every session
// end is followed by a new session after a jittered exponential backoff, so a
// Tibber outage neither restarts the exporter nor hammers Tibber.
func (h *Home) RunLiveFeed(ctx context.Context, f LiveFeed) {
	f.setDefaults()
	for {
		start := time.Now()
		reason, gotData, err := h.subscribeOnce(ctx, f)
		if ctx.Err() != nil {
			break
		}
		if f.OnEnd != nil {
			f.OnEnd(reason)
		}
		if gotData && time.Since(start) >= f.healthy {
			f.backoff.Reset()
		}
		d := f.backoff.Next()
		if err != nil {
			reason = fmt.Sprintf("%s (%v)", reason, err)
		}
		log.Printf("Live feed for home %v ended: %s. Reconnecting in %s\n", h.Id, reason, d.Round(time.Millisecond))
		if !backoff.Sleep(ctx, d) {
			break
		}
	}
	log.Printf("Live feed for home %v stopped\n", h.Id)
}

// subscribeOnce runs one session on its own SubscriptionClient and returns
// why it ended, whether it delivered new data, and the error behind the
// reason, if there is one.
func (h *Home) subscribeOnce(ctx context.Context, f LiveFeed) (string, bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &session{}
	h.setLiveOpen(true)
	defer h.setLiveOpen(false)
	// Cancelling ctx strands the library's run goroutine (a few KB) when a read
	// error comes in at the same moment. For max age that takes a drop within
	// about 100 µs of the deadline, once a day, so it is accepted here.
	maxAge := time.AfterFunc(f.maxAge, func() {
		s.end(EndMaxAge)
		cancel()
	})
	defer maxAge.Stop()
	// The read timeout only notices silence. Keep-alives, errors without
	// complete and repeated measurements keep the socket busy without data.
	stalled := time.AfterFunc(f.ReadTimeout, func() {
		s.end(EndReadTimeout)
		s.abort(cancel)
	})
	defer stalled.Stop()

	log.Printf("Creating measurements subscription for home %v\n", h.Id)
	subscriber := newSubscriber(f.Url, f.HTTPClient, f.Token)
	subscriber.WithReadTimeout(f.ReadTimeout)
	subscriber.WithWebSocket(s.dial)
	subscriber.OnConnected(func() {
		log.Printf("Measurements subscription for home %v connected\n", h.Id)
	})
	// The library would reconnect by itself after a fixed delay. Stop it:
	// RunLiveFeed owns the retry policy.
	subscriber.OnDisconnected(func() {
		log.Printf("Measurements subscription for home %v disconnected\n", h.Id)
		cancel()
	})
	subscriber.OnError(func(sc *graphql.SubscriptionClient, err error) error {
		log.Printf("OnError: %v\n", err)
		s.fail(err)
		return err
	})
	_, err := subscriber.Subscribe(
		&Measurements{},
		map[string]interface{}{
			"id": h.Id,
		},
		func(dataValue []byte, errValue error) error {
			if errValue != nil {
				// Tibber usually follows an error with complete, which ends
				// the session.
				log.Printf("Subscription error for home %v: %v\n", h.Id, errValue)
				s.setGraphQLError(true)
				return nil
			}
			var m Measurements
			if err := json.Unmarshal(dataValue, &m); err != nil {
				log.Println(err)
				return nil
			}
			if h.applyMeasurement(m, time.Now()) {
				// The stream went on after an earlier error.
				s.setGraphQLError(false)
				s.gotData.Store(true)
				stalled.Reset(f.ReadTimeout)
			}
			return nil
		})
	if err != nil {
		return "subscribe_failed", false, err
	}
	err = subscriber.RunWithContext(ctx)
	// After Tibber's complete the library closes the socket on its own
	// goroutine, and RunWithContext returns before that is done. Wait for it,
	// so that the next session never overlaps this one. Cancel first, so that
	// the close does not count as a read error.
	cancel()
	s.close()
	_ = subscriber.Close()
	reason, err := s.reason(err)
	return reason, s.gotData.Load(), err
}

// newSubscriber returns a client for one live feed session. It logs the
// protocol messages, except connection_init: that message carries the API token.
func newSubscriber(wsUrl string, hc *http.Client, token string) *graphql.SubscriptionClient {
	subscriber := graphql.NewSubscriptionClient(wsUrl)
	subscriber.WithProtocol(graphql.GraphQLWS)
	subscriber.WithConnectionParams(map[string]interface{}{"token": token})
	subscriber.WithSyncMode(true)
	subscriber.WithLog(log.Println)
	// next carries every measurement. The subscription handler logs Tibber's
	// errors instead.
	subscriber.WithoutLogTypes(graphql.GQLConnectionInit, graphql.GQLNext)
	// Give up after the first failed dial (0 would retry forever).
	subscriber.WithRetryTimeout(time.Nanosecond)
	subscriber.WithRetryDelay(0)
	// The read timeout also covers a missing connection_ack.
	subscriber.WithConnectionInitialisationTimeout(0)
	subscriber.WithWebSocketOptions(graphql.WebsocketOptions{HTTPClient: hc})
	return subscriber
}

// session records why one websocket session ended.
type session struct {
	mu       sync.Mutex
	cause    string // set by the exporter: max_age or read_timeout
	gqlError bool   // Tibber sent an error, and no new data came after it
	err      error  // first dial, read or protocol error
	conn     *wsConn
	gotData  atomic.Bool
}

type dialError struct{ error }

func (e dialError) Unwrap() error { return e.error }

func (s *session) end(cause string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cause == "" {
		s.cause = cause
	}
}

func (s *session) setGraphQLError(pending bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gqlError = pending
}

func (s *session) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// reason returns why the session ended, and the error behind that if the
// exporter did not end it.
func (s *session) reason(runErr error) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gqlError {
		return EndGraphQLError, nil
	}
	if s.cause != "" {
		return s.cause, nil
	}
	err := s.err
	if err == nil {
		err = runErr
	}
	var de dialError
	var ce websocket.CloseError
	switch {
	case err == nil:
		return EndComplete, nil
	case errors.As(err, &de):
		return EndDialFailed, err
	case errors.Is(err, context.DeadlineExceeded):
		return EndReadTimeout, err
	case errors.As(err, &ce):
		return EndServerClose, err
	default:
		return EndConnectionLost, err
	}
}

// abort drops the connection, and the library then ends the session as after
// a read error. Cancelling ctx instead would race with the read timeout,
// which fires at about the same time on a silent connection (see maxAge).
func (s *session) abort(cancel func()) {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		cancel() // still dialing
		return
	}
	_ = c.CloseNow()
}

// close closes the connection with a close frame. If the library is already
// closing it, close waits until that is done.
func (s *session) close() {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// dial opens the websocket for the library, wrapped in a wsConn.
func (s *session) dial(ctx context.Context, url string, o graphql.WebsocketOptions) (graphql.WebsocketConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient:   o.HTTPClient,
		HTTPHeader:   o.HTTPHeader,
		Subprotocols: o.Subprotocols,
	})
	if err != nil {
		if ctx.Err() == nil {
			s.fail(dialError{err})
		}
		return nil, err
	}
	wc := &wsConn{Conn: c, ctx: ctx, readTimeout: o.ReadTimeout, writeTimeout: o.WriteTimeout, onReadError: s.fail}
	s.mu.Lock()
	s.conn = wc
	s.mu.Unlock()
	return wc, nil
}

// wsConn works like the library's default connection, with two changes. It
// records read errors, so the session knows why it ended: the library only
// logs most of them and reconnects by itself. And its reads and writes do not
// end with the session: coder/websocket drops the connection when a read's
// context ends, and then the library cannot send a close frame on shutdown.
type wsConn struct {
	*websocket.Conn
	ctx          context.Context // the session
	readTimeout  time.Duration
	writeTimeout time.Duration
	onReadError  func(error)
}

func (c *wsConn) ReadJSON(v any) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.readTimeout)
	defer cancel()
	err := wsjson.Read(ctx, c.Conn, v)
	if err != nil && c.ctx.Err() != nil {
		// The exporter ended the session and closed the connection.
		return context.Canceled
	}
	if err != nil {
		c.onReadError(err)
	}
	return err
}

func (c *wsConn) WriteJSON(v any) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.writeTimeout)
	defer cancel()
	return wsjson.Write(ctx, c.Conn, v)
}

func (c *wsConn) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.Conn.Ping(ctx)
}

func (c *wsConn) Close() error {
	return c.Conn.Close(websocket.StatusNormalClosure, "")
}

// GetCloseStatus maps a read timeout to 1005, as the library does:
// coder/websocket closes the connection when a read times out.
func (c *wsConn) GetCloseStatus(err error) int32 {
	if errors.Is(err, context.DeadlineExceeded) {
		return int32(websocket.StatusNoStatusRcvd)
	}
	return int32(websocket.CloseStatus(err))
}
