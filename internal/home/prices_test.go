package home

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hasura/go-graphql-client"
)

func pricesBody(total float64) string {
	start := time.Now().Truncate(time.Hour).Format(time.RFC3339)
	price := fmt.Sprintf(`{"startsAt":%q,"total":%v,"energy":0.1,"tax":0.1,"level":"NORMAL"}`, start, total)
	return `{"data":{"viewer":{"home":{"currentSubscription":{"priceInfo":{"current":` + price + `,"today":[` + price + `],"tomorrow":[]}}}}}}`
}

const validationError = `{"errors":[{"message":"Unknown argument \"resolution\" on field \"Subscription.priceInfo\".","extensions":{"code":"GRAPHQL_VALIDATION_FAILED"}}]}`

// The hourly fallback is for markets without QUARTER_HOURLY. A transient
// error must keep the last prices instead of swapping in hourly ones.
func TestUpdatePricesFallback(t *testing.T) {
	captureLog(t)
	var quarterHourly atomic.Pointer[func(w http.ResponseWriter)]
	var hourlyCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "QUARTER_HOURLY") {
			(*quarterHourly.Load())(w)
			return
		}
		hourlyCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, pricesBody(0.5))
	}))
	defer srv.Close()
	client := graphql.NewClient(srv.URL, &http.Client{Timeout: 100 * time.Millisecond})

	status := func(code int, body string) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			fmt.Fprint(w, body)
		}
	}
	cases := []struct {
		name       string
		response   func(w http.ResponseWriter)
		wantHourly bool
	}{
		{"502", status(http.StatusBadGateway, "Bad Gateway"), false},
		{"504", status(http.StatusGatewayTimeout, "Gateway Timeout"), false},
		{"timeout", func(w http.ResponseWriter) { time.Sleep(300 * time.Millisecond) }, false},
		{"reset", func(w http.ResponseWriter) {
			c, _, _ := w.(http.Hijacker).Hijack()
			_ = c.(*net.TCPConn).SetLinger(0)
			_ = c.Close()
		}, false},
		{"validation error", status(http.StatusOK, validationError), true},
		{"validation error with 400", status(http.StatusBadRequest, validationError), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := New("home")
			ok := status(http.StatusOK, pricesBody(0.25))
			quarterHourly.Store(&ok)
			h.UpdatePrices(context.Background(), client)
			quarterHourly.Store(&c.response)
			hourlyCalls.Store(0)
			h.UpdatePrices(context.Background(), client)

			want := 0.25
			if c.wantHourly {
				want = 0.5
			}
			if got := *h.PricesSnapshot().Viewer.Home.CurrentSubscription.PriceInfo.Current.Total; got != want {
				t.Errorf("price = %v, want %v", got, want)
			}
			if got := hourlyCalls.Load() == 1; got != c.wantHourly {
				t.Errorf("hourly query called: %v, want %v", got, c.wantHourly)
			}
		})
	}
}
