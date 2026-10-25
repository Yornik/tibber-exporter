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
	// The other protocol messages are still logged.
	if !strings.Contains(logs.String(), `"type":"subscribe"`) {
		t.Fatalf("the subscribe message is not in the log:\n%s", logs.String())
	}
}
