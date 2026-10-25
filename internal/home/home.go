package home

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hasura/go-graphql-client"
	"github.com/terjesannum/tibber-exporter/internal/tibber"
)

type Measurements struct {
	LiveMeasurement tibber.LiveMeasurement `graphql:"liveMeasurement(homeId: $id)"`
}

type GaugeValues struct {
	CostToday   float64
	RewardToday *float64
}

type Home struct {
	Id                graphql.ID
	Prices            tibber.Prices `graphql:"home(id: $id)"`
	PreviousHour      tibber.PreviousPower
	PreviousDay       tibber.PreviousPower
	Measurements      Measurements
	TimestampedValues tibber.TimestampedValues
	GaugeValues       GaugeValues

	// mu guards the fields above and below: the live feed, the price ticker,
	// the collectors and the HTTP handlers run concurrently. Read them with
	// the snapshot methods.
	mu          sync.RWMutex
	liveOpen    bool      // a live feed session is open
	liveStart   time.Time // when it was opened
	lastMessage time.Time // receive time of the last new live measurement
}

func New(id graphql.ID) *Home {
	return &Home{
		Id: id,
	}
}

func (h *Home) UpdatePrices(ctx context.Context, client *graphql.Client) {
	log.Printf("Updating prices for %v\n", h.Id)
	// Prefer 15-minute (quarter-hourly) price resolution. Fall back to the
	// hourly query only when Tibber rejects QUARTER_HOURLY. On a transient
	// error (5xx, timeout, reset) keep the last prices.
	var qh tibber.PricesQuarterHourly
	if err := client.Query(ctx, &qh, map[string]interface{}{"id": h.Id}); err != nil {
		if !quarterHourlyUnsupported(err) {
			log.Printf("Price query failed for %v, keeping previous prices: %v\n", h.Id, err)
			return
		}
		log.Printf("Quarter-hourly price query failed for %v, falling back to hourly: %v\n", h.Id, err)
		var prices tibber.Prices
		if err := client.Query(ctx, &prices, map[string]interface{}{"id": h.Id}); err != nil {
			log.Println(err)
			return
		}
		h.mu.Lock()
		h.Prices = prices
		h.mu.Unlock()
		return
	}
	h.mu.Lock()
	h.Prices.Viewer.Home.CurrentSubscription.PriceInfo.Current = qh.Viewer.Home.CurrentSubscription.PriceInfo.Current
	h.Prices.Viewer.Home.CurrentSubscription.PriceInfo.Today = qh.Viewer.Home.CurrentSubscription.PriceInfo.Today
	h.Prices.Viewer.Home.CurrentSubscription.PriceInfo.Tomorrow = qh.Viewer.Home.CurrentSubscription.PriceInfo.Tomorrow
	h.mu.Unlock()
}

// quarterHourlyUnsupported reports whether Tibber rejected the query as
// invalid (a GraphQL validation error, or an error that names the
// resolution), as opposed to a transient failure.
func quarterHourlyUnsupported(err error) bool {
	var errs graphql.Errors
	if !errors.As(err, &errs) {
		return false
	}
	for _, e := range errs {
		text := fmt.Sprint(e.Message, " ", e.Extensions["code"])
		var ne graphql.NetworkError
		if errors.As(e, &ne) {
			if ne.StatusCode() != http.StatusBadRequest {
				continue // 5xx, 429 and the like
			}
			text = ne.Body()
		} else if e.Extensions["code"] == graphql.ErrRequestError {
			continue // timeout, reset, DNS
		}
		text = strings.ToLower(text)
		if strings.Contains(text, "resolution") || strings.Contains(text, "quarter_hourly") || strings.Contains(text, "graphql_validation_failed") {
			return true
		}
	}
	return false
}

// PricesSnapshot returns a copy that is safe to read without the lock.
// UpdatePrices replaces the slices, it never changes them.
func (h *Home) PricesSnapshot() tibber.Prices {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.Prices
}

// PreviousSnapshot returns copies of the previous hour and day values.
func (h *Home) PreviousSnapshot() (hour, day tibber.PreviousPower) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.PreviousHour, h.PreviousDay
}

// LiveSnapshot returns copies of the live values.
func (h *Home) LiveSnapshot() (tibber.LiveMeasurement, tibber.TimestampedValues, GaugeValues) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.Measurements.LiveMeasurement, h.TimestampedValues, h.GaugeValues
}

// LiveStatus reports whether a live feed session is open and has delivered a
// new measurement, and the receive time of the last new measurement.
func (h *Home) LiveStatus() (connected bool, lastMessage time.Time) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.liveOpen && !h.lastMessage.Before(h.liveStart), h.lastMessage
}

func (h *Home) setLiveOpen(open bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.liveOpen = open
	h.liveStart = time.Now()
}

func (h *Home) GetPricesHandler(w http.ResponseWriter, r *http.Request) {
	pi := h.PricesSnapshot().Viewer.Home.CurrentSubscription.PriceInfo
	var prices []tibber.Price
	if r.FormValue("period") == "today" {
		prices = pi.Today
	} else if r.FormValue("period") == "tomorrow" {
		prices = pi.Tomorrow
	} else if r.FormValue("period") == "now" {
		prices = h.GetPrices(time.Now())
	} else {
		prices = slices.Concat(pi.Today, pi.Tomorrow)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(prices)
}

func (h *Home) GetPrices(t time.Time) []tibber.Price {
	pi := h.PricesSnapshot().Viewer.Home.CurrentSubscription.PriceInfo
	p := slices.Concat(pi.Today, pi.Tomorrow)
	i := 0
	for i < len(p) && t.After(*p[i].StartsAt) {
		i++
	}
	if i < len(p) {
		return p[i-1:]
	}
	return nil
}

// latestPeriod reports whether a node that ended at to is still the latest
// complete hour or day. A day node may be up to 25 hours old: the day of the
// switch from summer to winter time has 25 hours, and a 24-hour limit drops
// the previous day for the last hour of that day.
func latestPeriod(res tibber.EnergyResolution, to, now time.Time) bool {
	age := now.Sub(to)
	if res == tibber.ResolutionDaily {
		return age < 25*time.Hour
	}
	return age < time.Hour
}

func (h *Home) UpdatePrevious(ctx context.Context, client *graphql.Client, res tibber.EnergyResolution) {
	var prev tibber.PreviousQuery
	log.Printf("Updating %v data for %v\n", res, h.Id)
	err := client.Query(ctx, &prev, map[string]interface{}{
		"id":         h.Id,
		"resolution": res,
	})
	if err != nil {
		log.Println(err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	values := &h.PreviousHour
	if res == tibber.ResolutionDaily {
		values = &h.PreviousDay
	}
	if len(prev.Viewer.Home.Consumption.Nodes) == 0 {
		values.Consumption = nil
		values.Cost = nil
	} else {
		node := prev.Viewer.Home.Consumption.Nodes[0]
		now := time.Now()
		values.Timestamp = now
		if latestPeriod(res, node.To, now) {
			values.Consumption = node.Consumption
			values.Cost = node.Cost
		} else {
			values.Consumption = nil
			values.Cost = nil
		}
	}
	if len(prev.Viewer.Home.Production.Nodes) == 0 {
		values.Production = nil
		values.Profit = nil
	} else {
		node := prev.Viewer.Home.Production.Nodes[0]
		now := time.Now()
		values.Timestamp = now
		if latestPeriod(res, node.To, now) {
			values.Production = node.Production
			values.Profit = node.Profit
		} else {
			values.Production = nil
			values.Profit = nil
		}
	}
}

// applyMeasurement stores one live measurement, received at the given time,
// and reports whether it was new: a repeated or older timestamp is not.
func (h *Home) applyMeasurement(m Measurements, received time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Tibber sometimes delivers an older message after a newer one. Skip it, so
	// the stored timestamp never moves backwards. A stored timestamp more than a
	// minute ahead of the clock is not trusted, so one bad reading cannot
	// freeze the feed.
	stored := h.Measurements.LiveMeasurement.Timestamp
	if m.LiveMeasurement.Timestamp.Before(stored) && !stored.After(received.Add(time.Minute)) {
		log.Printf("Skipping out-of-order measurement for home %v: %s < %s\n", h.Id, m.LiveMeasurement.Timestamp, stored)
		return false
	}
	isNew := !m.LiveMeasurement.Timestamp.Equal(stored)
	if isNew {
		h.lastMessage = received
	}
	h.Measurements.LiveMeasurement.Power = m.LiveMeasurement.Power
	h.Measurements.LiveMeasurement.MinPower = m.LiveMeasurement.MinPower
	h.Measurements.LiveMeasurement.MaxPower = m.LiveMeasurement.MaxPower
	h.Measurements.LiveMeasurement.AveragePower = m.LiveMeasurement.AveragePower
	h.Measurements.LiveMeasurement.PowerProduction = m.LiveMeasurement.PowerProduction
	h.Measurements.LiveMeasurement.MinPowerProduction = m.LiveMeasurement.MinPowerProduction
	h.Measurements.LiveMeasurement.MaxPowerProduction = m.LiveMeasurement.MaxPowerProduction
	// Each hour tibber seems to adjust readings (to official hourly reading?) and the accumulated values could be a bit lower that the previous.
	// This causes problems for prometheus counters, so skip those values.
	if m.LiveMeasurement.AccumulatedConsumption >= h.Measurements.LiveMeasurement.AccumulatedConsumption ||
		m.LiveMeasurement.Timestamp.YearDay() != h.Measurements.LiveMeasurement.Timestamp.YearDay() {
		h.Measurements.LiveMeasurement.AccumulatedConsumption = m.LiveMeasurement.AccumulatedConsumption
	} else {
		log.Printf("Accumulated consumption lower than stored value: %f(%s) < %f(%s)\n",
			m.LiveMeasurement.AccumulatedConsumption, m.LiveMeasurement.Timestamp, h.Measurements.LiveMeasurement.AccumulatedConsumption, h.Measurements.LiveMeasurement.Timestamp)
	}
	if m.LiveMeasurement.AccumulatedCost >= h.Measurements.LiveMeasurement.AccumulatedCost ||
		m.LiveMeasurement.Timestamp.YearDay() != h.Measurements.LiveMeasurement.Timestamp.YearDay() {
		h.Measurements.LiveMeasurement.AccumulatedCost = m.LiveMeasurement.AccumulatedCost
	} else {
		log.Printf("Accumulated cost lower than stored value: %f(%s) < %f(%s)\n",
			m.LiveMeasurement.AccumulatedCost, m.LiveMeasurement.Timestamp, h.Measurements.LiveMeasurement.AccumulatedCost, h.Measurements.LiveMeasurement.Timestamp)
	}
	h.GaugeValues.CostToday = m.LiveMeasurement.AccumulatedCost
	if m.LiveMeasurement.AccumulatedProduction >= h.Measurements.LiveMeasurement.AccumulatedProduction ||
		m.LiveMeasurement.Timestamp.YearDay() != h.Measurements.LiveMeasurement.Timestamp.YearDay() {
		h.Measurements.LiveMeasurement.AccumulatedProduction = m.LiveMeasurement.AccumulatedProduction
	} else {
		log.Printf("Accumulated production lower than stored value: %f(%s) < %f(%s)\n",
			m.LiveMeasurement.AccumulatedProduction, m.LiveMeasurement.Timestamp, h.Measurements.LiveMeasurement.AccumulatedProduction, h.Measurements.LiveMeasurement.Timestamp)
	}
	if m.LiveMeasurement.AccumulatedReward != nil {
		if h.Measurements.LiveMeasurement.AccumulatedReward == nil || *m.LiveMeasurement.AccumulatedReward >= *h.Measurements.LiveMeasurement.AccumulatedReward ||
			m.LiveMeasurement.Timestamp.YearDay() != h.Measurements.LiveMeasurement.Timestamp.YearDay() {
			h.Measurements.LiveMeasurement.AccumulatedReward = m.LiveMeasurement.AccumulatedReward
		} else {
			log.Printf("Accumulated reward lower than stored value: %f(%s) < %f(%s)\n",
				*m.LiveMeasurement.AccumulatedReward, m.LiveMeasurement.Timestamp, *h.Measurements.LiveMeasurement.AccumulatedReward, h.Measurements.LiveMeasurement.Timestamp)
		}
	}
	h.GaugeValues.RewardToday = m.LiveMeasurement.AccumulatedReward
	if m.LiveMeasurement.CurrentL1 != nil {
		h.TimestampedValues.CurrentL1.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.CurrentL1.Value = *m.LiveMeasurement.CurrentL1
	}
	if m.LiveMeasurement.CurrentL2 != nil {
		h.TimestampedValues.CurrentL2.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.CurrentL2.Value = *m.LiveMeasurement.CurrentL2
	}
	if m.LiveMeasurement.CurrentL3 != nil {
		h.TimestampedValues.CurrentL3.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.CurrentL3.Value = *m.LiveMeasurement.CurrentL3
	}
	if m.LiveMeasurement.VoltagePhase1 != nil {
		h.TimestampedValues.VoltagePhase1.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.VoltagePhase1.Value = *m.LiveMeasurement.VoltagePhase1
	}
	if m.LiveMeasurement.VoltagePhase2 != nil {
		h.TimestampedValues.VoltagePhase2.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.VoltagePhase2.Value = *m.LiveMeasurement.VoltagePhase2
	}
	if m.LiveMeasurement.VoltagePhase3 != nil {
		h.TimestampedValues.VoltagePhase3.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.VoltagePhase3.Value = *m.LiveMeasurement.VoltagePhase3
	}
	if m.LiveMeasurement.SignalStrength != nil {
		h.TimestampedValues.SignalStrength.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.SignalStrength.Value = *m.LiveMeasurement.SignalStrength
	}
	if m.LiveMeasurement.PowerReactive != nil {
		h.TimestampedValues.PowerReactive.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.PowerReactive.Value = *m.LiveMeasurement.PowerReactive
	}
	if m.LiveMeasurement.PowerProductionReactive != nil {
		h.TimestampedValues.PowerProductionReactive.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.PowerProductionReactive.Value = *m.LiveMeasurement.PowerProductionReactive
	}
	if m.LiveMeasurement.PowerFactor != nil {
		h.TimestampedValues.PowerFactor.Timestamp = m.LiveMeasurement.Timestamp
		h.TimestampedValues.PowerFactor.Value = *m.LiveMeasurement.PowerFactor
	}
	if m.LiveMeasurement.LastMeterConsumption >= h.Measurements.LiveMeasurement.LastMeterConsumption {
		h.Measurements.LiveMeasurement.LastMeterConsumption = m.LiveMeasurement.LastMeterConsumption
	}
	if m.LiveMeasurement.LastMeterProduction >= h.Measurements.LiveMeasurement.LastMeterProduction {
		h.Measurements.LiveMeasurement.LastMeterProduction = m.LiveMeasurement.LastMeterProduction
	}
	h.Measurements.LiveMeasurement.Timestamp = m.LiveMeasurement.Timestamp
	return isNew
}
