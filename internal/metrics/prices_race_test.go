package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/terjesannum/tibber-exporter/internal/home"
	"github.com/terjesannum/tibber-exporter/internal/tibber"
)

func fakeAPI(t *testing.T) *graphql.Client {
	day := time.Now().Truncate(24 * time.Hour)
	var prices []string
	for i := 0; i < 96; i++ {
		prices = append(prices, fmt.Sprintf(`{"startsAt":%q,"total":0.%02d,"energy":0.1,"tax":0.1,"level":"NORMAL"}`, day.Add(time.Duration(i)*15*time.Minute).Format(time.RFC3339), i))
	}
	pricesBody := `{"data":{"viewer":{"home":{"currentSubscription":{"priceInfo":{"current":{"startsAt":"` + day.Format(time.RFC3339) +
		`","total":0.2,"energy":0.1,"tax":0.1,"level":"NORMAL"},"today":[` + strings.Join(prices, ",") + `],"tomorrow":[]}}}}}}`
	to := time.Now().Truncate(time.Hour)
	previousBody := fmt.Sprintf(`{"data":{"viewer":{"home":{"consumption":{"nodes":[{"from":%q,"to":%q,"consumption":1.5,"cost":0.3}]},"production":{"nodes":[]}}}}}`,
		to.Add(-time.Hour).Format(time.RFC3339), to.Format(time.RFC3339))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(string(body), "consumption(") {
			w.Write([]byte(previousBody))
			return
		}
		w.Write([]byte(pricesBody))
	}))
	t.Cleanup(srv.Close)
	return graphql.NewClient(srv.URL, nil)
}

// The price ticker writes while /metrics and /homes/<id>/prices read, as in
// production. Run with -race.
func TestPricesRace(t *testing.T) {
	client := fakeAPI(t)
	h := home.New("race-home")
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewHomeCollector(h))
	stop := time.Now().Add(time.Second)
	var wg sync.WaitGroup
	wg.Go(func() { // main.go ticker goroutine
		for time.Now().Before(stop) {
			h.UpdatePrices(context.Background(), client)
			h.UpdatePrevious(context.Background(), client, tibber.ResolutionHourly)
		}
	})
	for time.Now().Before(stop) { // promhttp and the price handler
		if _, err := reg.Gather(); err != nil {
			t.Fatal(err)
		}
		h.GetPrices(time.Now())
		for _, period := range []string{"", "now", "today", "tomorrow"} {
			h.GetPricesHandler(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/?period="+period, nil))
		}
	}
	wg.Wait()
	labels := map[string]string{"home_id": "race-home", "type": "total"}
	if v, _ := value(t, reg, "tibber_power_price", labels); v != 0.2 {
		t.Errorf("tibber_power_price = %v, want 0.2", v)
	}
	if v, _ := value(t, reg, "tibber_power_consumption_previous_hour", labels); v != 1.5 {
		t.Errorf("tibber_power_consumption_previous_hour = %v, want 1.5", v)
	}
}
