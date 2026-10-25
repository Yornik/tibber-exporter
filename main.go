package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/prometheus/client_golang/prometheus"
	promver "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"github.com/terjesannum/tibber-exporter/internal/backoff"
	"github.com/terjesannum/tibber-exporter/internal/home"
	"github.com/terjesannum/tibber-exporter/internal/metrics"
	"github.com/terjesannum/tibber-exporter/internal/tibber"
)

var (
	token                    string
	homesQuery               tibber.HomesQuery
	liveFeedTimeout          int
	liveUrl                  string
	liveMeasurements         stringArgs
	disableLiveMeasurements  stringArgs
	disableSubscriptionCheck bool
	showVersion              bool
	listenAddress            string
	userAgent                string
)

type (
	stringArgs []string
	transport  struct {
		Token     string
		UserAgent string
		base      http.RoundTripper // nil means http.DefaultTransport
	}
)

// The API token goes in the Authorization header only to these hosts. The live
// feed also sends it in its connection_init message, to whatever host
// --live-url or Tibber's websocketSubscriptionUrl names; this list does not
// cover that.
var tibberHosts = []string{"api.tibber.com", "websocket-api.tibber.com"}

func (sa *stringArgs) String() string {
	return fmt.Sprintln(*sa)
}

func (sa *stringArgs) Set(s string) error {
	*sa = append(*sa, s)
	return nil
}

func init() {
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit")
	flag.StringVar(&token, "token", "", "Tibber API token (default $TIBBER_TOKEN)")
	flag.IntVar(&liveFeedTimeout, "live-feed-timeout", 1, "Minutes without a new measurement before the live feed reconnects (at least 1)")
	flag.StringVar(&liveUrl, "live-url", "", "Override websocket url for live measurements (the Authorization header goes only to Tibber hosts)")
	flag.Var(&liveMeasurements, "live", "Ids of homes to always start live measurements")
	flag.Var(&disableLiveMeasurements, "disable-live", "Ids of homes to disable live measurements")
	flag.BoolVar(&disableSubscriptionCheck, "disable-subscription-check", false, "Disable check on active Tibber subscription")
	flag.StringVar(&listenAddress, "listen-address", ":8080", "Address to listen on for HTTP requests")
}

// parseFlags is not part of init, so that the tests can run: the testing
// package registers its flags after init.
func parseFlags() {
	flag.Parse()
	if liveFeedTimeout < 1 {
		liveFeedTimeout = 1
	}
	// Not the flag default: the usage text prints the default.
	if token == "" {
		token = os.Getenv("TIBBER_TOKEN")
	}
	if version.Version == "" {
		userAgent = "tibber-exporter (https://github.com/terjesannum/tibber-exporter)"
	} else {
		userAgent = fmt.Sprintf("tibber-exporter/%s (https://github.com/terjesannum/tibber-exporter)", version.Version)
	}

	//Get homes with Forcefully enabled/disabled live Measurement Feature from Environment Variable and append them to slice
	envEnabledHomes, envPresent := os.LookupEnv("TIBBER_LIVE_ENABLE_HOMES")
	if envPresent {
		envEnabledHomes := strings.Split(envEnabledHomes, ",")
		for _, enabledHome := range envEnabledHomes {
			if len(enabledHome) >= 1 {
				liveMeasurements = append(liveMeasurements, enabledHome)
			}
		}
	}
	envDisabledHomes, envPresent := os.LookupEnv("TIBBER_LIVE_DISABLE_HOMES")
	if envPresent {
		envDisabledHomes := strings.Split(envDisabledHomes, ",")
		for _, disabledHome := range envDisabledHomes {
			if len(disabledHome) >= 1 {
				disableLiveMeasurements = append(disableLiveMeasurements, disabledHome)
			}
		}
	}
}

func exit(msg string) {
	log.Println(msg)
	time.Sleep(10 * time.Second)
	os.Exit(1)
}

// RoundTrip works on a copy: a RoundTripper must not change the caller's request.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if slices.Contains(tibberHosts, r.URL.Hostname()) {
		r.Header.Set("Authorization", fmt.Sprintf("Bearer %s", t.Token))
	}
	r.Header.Set("User-Agent", t.UserAgent)
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(r)
}

func getHomesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(homesQuery)
}

// queryHomes retries until Tibber answers, so a Tibber outage at start-up
// does not restart the exporter. It only fails when ctx ends.
func queryHomes(ctx context.Context, client *graphql.Client, b *backoff.Backoff) (tibber.HomesQuery, error) {
	for {
		var hq tibber.HomesQuery
		err := client.Query(ctx, &hq, nil)
		if err == nil {
			return hq, nil
		}
		if ctx.Err() != nil {
			return hq, ctx.Err()
		}
		d := b.Next()
		log.Printf("Error getting homes: %v. Retrying in %s\n", err, d.Round(time.Millisecond))
		if !backoff.Sleep(ctx, d) {
			return hq, ctx.Err()
		}
	}
}

// listen starts the HTTP listener before the first Tibber call, so /healthz
// and /metrics answer while Tibber is slow or down.
func listen(addr string) *http.Server {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Tibber exporter")
	})
	// Ready means the exporter serves HTTP. It does not depend on Tibber: a
	// restart or an unready pod does not fix a Tibber outage. The live feed
	// metrics show the state of the feed.
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.Handle("/metrics", promhttp.Handler())
	l, err := net.Listen("tcp", addr)
	if err != nil {
		exit(fmt.Sprintf("Error: %v", err))
	}
	log.Printf("Starting http listener %s\n", addr)
	srv := &http.Server{}
	go func() {
		if err := srv.Serve(l); err != http.ErrServerClosed {
			exit(fmt.Sprintf("Error: %v", err))
		}
	}()
	return srv
}

func main() {
	parseFlags()
	if showVersion {
		fmt.Printf("%s\n", version.Print("tibber-exporter"))
		os.Exit(0)
	}
	log.Printf("Starting %s\n", userAgent)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Without a timeout a stalled connection blocks the price ticker forever.
	// coder/websocket applies it to the websocket handshake only.
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &transport{Token: token, UserAgent: userAgent}}
	client := graphql.NewClient("https://api.tibber.com/v1-beta/gql", hc)
	prometheus.MustRegister(promver.NewCollector("tibber_exporter"))
	srv := listen(listenAddress)
	defer shutdown(srv)

	var err error
	homesQuery, err = queryHomes(ctx, client, backoff.New())
	if err != nil {
		stop()
		log.Println("Shutting down")
		return
	}
	http.HandleFunc("/homes", getHomesHandler)
	wsUrl := homesQuery.Viewer.WebsocketSubscriptionUrl
	log.Printf("Websocket url: %s\n", wsUrl)
	if liveUrl != "" && liveUrl != wsUrl {
		log.Printf("Overiding websocket url with: %s\n", liveUrl)
		wsUrl = liveUrl
	}
	var started []string
	var feeds sync.WaitGroup
	for _, s := range homesQuery.Viewer.Homes {
		s := s
		log.Printf("Found home: %v - %v\n", s.Id, s.AppNickname)
		if s.CurrentSubscription.Id == nil {
			log.Printf("No subscription found for home %v\n", s.Id)
		}
		if s.CurrentSubscription.Id != nil || disableSubscriptionCheck {
			log.Printf("Starting monitoring of home: %v - %v\n", s.Id, s.AppNickname)
			if s.CurrentSubscription.Id == nil {
				log.Println("Current subscription: n/a")
			} else {
				log.Printf("Current subscription: %v\n", *s.CurrentSubscription.Id)
			}
			h := home.New(s.Id)
			metrics.HomeInfo.WithLabelValues(
				string(s.Id),
				s.AppNickname,
				s.Address.Address1,
				s.Address.Address2,
				s.Address.Address3,
				s.Address.PostalCode,
				s.Address.City,
				s.Address.Country,
				s.Address.Latitude,
				s.Address.Longitude,
				s.TimeZone,
				s.CurrentSubscription.PriceInfo.Current.Currency,
			).Set(1)
			metrics.GridInfo.WithLabelValues(
				string(s.Id),
				s.MeteringPointData.GridCompany,
				s.MeteringPointData.PriceAreaCode,
			).Set(1)

			prometheus.MustRegister(metrics.NewHomeCollector(h))
			log.Printf("Realtime consumption enabled for %v: %v\n", s.Id, s.Features.RealTimeConsumptionEnabled)
			if (s.Features.RealTimeConsumptionEnabled || slices.Contains(liveMeasurements, string(s.Id))) && !slices.Contains(disableLiveMeasurements, string(s.Id)) {
				log.Printf("Starting live measurements monitoring of home %v\n", s.Id)
				log.Printf("Live feed timeout: %v minute\n", liveFeedTimeout)
				id := string(s.Id)
				for _, r := range home.EndReasons {
					metrics.LiveReconnects.WithLabelValues(id, r)
				}
				feed := home.LiveFeed{
					HTTPClient:  hc,
					Url:         wsUrl,
					Token:       token,
					ReadTimeout: time.Duration(liveFeedTimeout) * time.Minute,
					OnEnd: func(reason string) {
						metrics.LiveReconnects.WithLabelValues(id, reason).Inc()
					},
				}
				feeds.Go(func() { h.RunLiveFeed(ctx, feed) })
				prometheus.MustRegister(metrics.NewMeasurementCollector(id, h))
				started = append(started, id)
			} else {
				log.Printf("Live measurements not available for home %v\n", s.Id)
			}
			h.UpdatePrices(ctx, client)
			http.HandleFunc(fmt.Sprintf("/homes/%s/prices", h.Id), h.GetPricesHandler)
			ticker := time.NewTicker(time.Minute)
			go func() {
				for {
					select {
					case <-ticker.C:
						if s.CurrentSubscription.Id != nil {
							h.UpdatePrices(ctx, client)
						}
						h.UpdatePrevious(ctx, client, tibber.ResolutionHourly)
						h.UpdatePrevious(ctx, client, tibber.ResolutionDaily)
					case <-ctx.Done():
						ticker.Stop()
						return
					}
				}
			}()
		}
	}

	// Exit if live monitoring of configured home for some reason hasn't started
	if len(liveMeasurements) > 0 {
		for _, l := range liveMeasurements {
			if !slices.Contains(started, l) {
				exit(fmt.Sprintf("Monitoring of home %s not started. Exiting...\n", l))
			}
		}
	}

	<-ctx.Done()
	// A second signal now ends the process at once.
	stop()
	log.Println("Shutting down")
	// The live feeds close their websockets before they return.
	feeds.Wait()
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
