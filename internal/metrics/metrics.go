package metrics

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/terjesannum/tibber-exporter/internal/home"
	"github.com/terjesannum/tibber-exporter/internal/tibber"
)

var (
	HomeInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tibber_home_info",
			Help: "Home info",
		},
		[]string{
			"home_id",
			"name",
			"address1",
			"address2",
			"address3",
			"postal_code",
			"city",
			"country",
			"latitude",
			"longitude",
			"timezone",
			"currency",
		})
	GridInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "tibber_grid_info",
			Help: "Grid info",
		},
		[]string{
			"home_id",
			"grid_company",
			"price_area_code",
		})
	LiveReconnects = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tibber_live_reconnects_total",
			Help: "Live feed sessions that ended, by reason",
		},
		[]string{
			"home_id",
			"reason",
		})
)

const maxAge = 5

type MeasurementCollector struct {
	home                 *home.Home
	stale                atomic.Bool // the last Collect found the measurements too old
	liveConnected        *prometheus.Desc
	liveLastMessage      *prometheus.Desc
	consumption          *prometheus.Desc
	consumptionMin       *prometheus.Desc
	consumptionMax       *prometheus.Desc
	consumptionAvg       *prometheus.Desc
	consumptionTotal     *prometheus.Desc
	costToday            *prometheus.Desc
	costTotal            *prometheus.Desc
	current              *prometheus.Desc
	voltage              *prometheus.Desc
	signalStrength       *prometheus.Desc
	production           *prometheus.Desc
	productionMin        *prometheus.Desc
	productionMax        *prometheus.Desc
	productionTotal      *prometheus.Desc
	rewardToday          *prometheus.Desc
	rewardTotal          *prometheus.Desc
	consumptionReactive  *prometheus.Desc
	productionReactive   *prometheus.Desc
	powerFactor          *prometheus.Desc
	lastMeterConsumption *prometheus.Desc
	lastMeterProduction  *prometheus.Desc
}

func NewMeasurementCollector(homeId string, h *home.Home) *MeasurementCollector {
	return &MeasurementCollector{
		home: h,
		liveConnected: prometheus.NewDesc(
			"tibber_live_connected",
			"1 while a live feed session is open and has delivered new data",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		liveLastMessage: prometheus.NewDesc(
			"tibber_live_last_message_timestamp_seconds",
			"Time the last new live measurement was received",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumption: prometheus.NewDesc(
			"tibber_power_consumption",
			"Power consumption",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumptionMin: prometheus.NewDesc(
			"tibber_power_consumption_day_min",
			"Minimum power consumption since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumptionMax: prometheus.NewDesc(
			"tibber_power_consumption_day_max",
			"Maximum power consumption since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumptionAvg: prometheus.NewDesc(
			"tibber_power_consumption_day_avg",
			"Average power consumtion since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumptionTotal: prometheus.NewDesc(
			"tibber_power_consumption_day_total",
			"Total power consumption since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		costToday: prometheus.NewDesc(
			"tibber_power_cost_day",
			"Total power cost since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		costTotal: prometheus.NewDesc(
			"tibber_power_cost_day_total",
			"Total power cost since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		current: prometheus.NewDesc(
			"tibber_current",
			"Line current",
			[]string{"line"},
			prometheus.Labels{"home_id": homeId},
		),
		voltage: prometheus.NewDesc(
			"tibber_voltage",
			"Phase voltage",
			[]string{"phase"},
			prometheus.Labels{"home_id": homeId},
		),
		signalStrength: prometheus.NewDesc(
			"tibber_signal_strength",
			"Signal strength",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		production: prometheus.NewDesc(
			"tibber_power_production",
			"Power production",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		productionMin: prometheus.NewDesc(
			"tibber_power_production_day_min",
			"Minimum power production since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		productionMax: prometheus.NewDesc(
			"tibber_power_production_day_max",
			"Maximum power production since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		productionTotal: prometheus.NewDesc(
			"tibber_power_production_day_total",
			"Total power production since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		rewardToday: prometheus.NewDesc(
			"tibber_power_production_reward_day",
			"Total power production reward since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		rewardTotal: prometheus.NewDesc(
			"tibber_power_production_reward_day_total",
			"Total power production reward since midnight",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		consumptionReactive: prometheus.NewDesc(
			"tibber_power_consumption_reactive",
			"Reactive consumption",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		productionReactive: prometheus.NewDesc(
			"tibber_power_production_reactive",
			"Reactive production",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		powerFactor: prometheus.NewDesc(
			"tibber_power_factor",
			"Power factor (active power / apparent power)",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		lastMeterConsumption: prometheus.NewDesc(
			"tibber_power_consumption_meter_total",
			"Last consumption meter state",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
		lastMeterProduction: prometheus.NewDesc(
			"tibber_power_production_meter_total",
			"Last production meter state",
			nil,
			prometheus.Labels{"home_id": homeId},
		),
	}
}

func (c *MeasurementCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.liveConnected
	ch <- c.liveLastMessage
	ch <- c.consumption
	ch <- c.consumptionMin
	ch <- c.consumptionMax
	ch <- c.consumptionAvg
	ch <- c.consumptionTotal
	ch <- c.costToday
	ch <- c.costTotal
	ch <- c.current
	ch <- c.voltage
	ch <- c.signalStrength
	ch <- c.production
	ch <- c.productionMin
	ch <- c.productionMax
	ch <- c.productionTotal
	ch <- c.rewardToday
	ch <- c.rewardTotal
	ch <- c.consumptionReactive
	ch <- c.productionReactive
	ch <- c.powerFactor
	ch <- c.lastMeterConsumption
	ch <- c.lastMeterProduction
}

func (c *MeasurementCollector) Collect(ch chan<- prometheus.Metric) {
	connected, lastMessage := c.home.LiveStatus()
	up := 0.0
	if connected {
		up = 1
	}
	ch <- prometheus.MustNewConstMetric(c.liveConnected, prometheus.GaugeValue, up)
	if !lastMessage.IsZero() {
		ch <- prometheus.MustNewConstMetric(c.liveLastMessage, prometheus.GaugeValue, float64(lastMessage.UnixNano())/1e9)
	}
	measurements, timestampedValues, gaugeValues := c.home.LiveSnapshot()
	timeDiff := time.Now().Sub(measurements.Timestamp)
	if timeDiff.Minutes() > maxAge {
		// Once per stale period: the scrape and the probes collect several
		// times a minute.
		if !c.stale.Swap(true) {
			log.Printf("Measurements too old: %s\n", measurements.Timestamp)
		}
		return
	}
	c.stale.Store(false)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.consumption,
			prometheus.GaugeValue,
			measurements.Power,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.consumptionMin,
			prometheus.GaugeValue,
			measurements.MinPower,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.consumptionMax,
			prometheus.GaugeValue,
			measurements.MaxPower,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.consumptionAvg,
			prometheus.GaugeValue,
			measurements.AveragePower,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.consumptionTotal,
			prometheus.CounterValue,
			measurements.AccumulatedConsumption,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.costToday,
			prometheus.GaugeValue,
			gaugeValues.CostToday,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.costTotal,
			prometheus.CounterValue,
			measurements.AccumulatedCost,
		),
	)
	timeDiff = time.Now().Sub(timestampedValues.CurrentL1.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.CurrentL1.Timestamp,
			prometheus.MustNewConstMetric(
				c.current,
				prometheus.GaugeValue,
				timestampedValues.CurrentL1.Value,
				"1",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.CurrentL2.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.CurrentL2.Timestamp,
			prometheus.MustNewConstMetric(
				c.current,
				prometheus.GaugeValue,
				timestampedValues.CurrentL2.Value,
				"2",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.CurrentL3.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.CurrentL3.Timestamp,
			prometheus.MustNewConstMetric(
				c.current,
				prometheus.GaugeValue,
				timestampedValues.CurrentL3.Value,
				"3",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.VoltagePhase1.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.VoltagePhase1.Timestamp,
			prometheus.MustNewConstMetric(
				c.voltage,
				prometheus.GaugeValue,
				timestampedValues.VoltagePhase1.Value,
				"1",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.VoltagePhase2.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.VoltagePhase2.Timestamp,
			prometheus.MustNewConstMetric(
				c.voltage,
				prometheus.GaugeValue,
				timestampedValues.VoltagePhase2.Value,
				"2",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.VoltagePhase3.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.VoltagePhase3.Timestamp,
			prometheus.MustNewConstMetric(
				c.voltage,
				prometheus.GaugeValue,
				timestampedValues.VoltagePhase3.Value,
				"3",
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.SignalStrength.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.SignalStrength.Timestamp,
			prometheus.MustNewConstMetric(
				c.signalStrength,
				prometheus.GaugeValue,
				timestampedValues.SignalStrength.Value,
			),
		)
	}
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.production,
			prometheus.GaugeValue,
			measurements.PowerProduction,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.productionMin,
			prometheus.GaugeValue,
			measurements.MinPowerProduction,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.productionMax,
			prometheus.GaugeValue,
			measurements.MaxPowerProduction,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.productionTotal,
			prometheus.CounterValue,
			measurements.AccumulatedProduction,
		),
	)
	if gaugeValues.RewardToday != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			measurements.Timestamp,
			prometheus.MustNewConstMetric(
				c.rewardToday,
				prometheus.GaugeValue,
				*gaugeValues.RewardToday,
			),
		)
	}
	if measurements.AccumulatedReward != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			measurements.Timestamp,
			prometheus.MustNewConstMetric(
				c.rewardTotal,
				prometheus.CounterValue,
				*measurements.AccumulatedReward,
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.PowerReactive.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.PowerReactive.Timestamp,
			prometheus.MustNewConstMetric(
				c.consumptionReactive,
				prometheus.GaugeValue,
				timestampedValues.PowerReactive.Value,
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.PowerProductionReactive.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.PowerProductionReactive.Timestamp,
			prometheus.MustNewConstMetric(
				c.productionReactive,
				prometheus.GaugeValue,
				timestampedValues.PowerProductionReactive.Value,
			),
		)
	}
	timeDiff = time.Now().Sub(timestampedValues.PowerFactor.Timestamp)
	if timeDiff.Minutes() < maxAge {
		ch <- prometheus.NewMetricWithTimestamp(
			timestampedValues.PowerFactor.Timestamp,
			prometheus.MustNewConstMetric(
				c.powerFactor,
				prometheus.GaugeValue,
				timestampedValues.PowerFactor.Value,
			),
		)
	}
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.lastMeterConsumption,
			prometheus.CounterValue,
			measurements.LastMeterConsumption,
		),
	)
	ch <- prometheus.NewMetricWithTimestamp(
		measurements.Timestamp,
		prometheus.MustNewConstMetric(
			c.lastMeterProduction,
			prometheus.CounterValue,
			measurements.LastMeterProduction,
		),
	)
}

type HomeCollector struct {
	home                    *home.Home
	price                   *prometheus.Desc
	priceLevel              *prometheus.Desc
	nextHourPrice           *prometheus.Desc
	nextHourPriceLevel      *prometheus.Desc
	previousHourConsumption *prometheus.Desc
	previousHourCost        *prometheus.Desc
	previousDayConsumption  *prometheus.Desc
	previousDayCost         *prometheus.Desc
	previousHourProduction  *prometheus.Desc
	previousHourProfit      *prometheus.Desc
	previousDayProduction   *prometheus.Desc
	previousDayProfit       *prometheus.Desc
}

func NewHomeCollector(home *home.Home) *HomeCollector {
	log.Printf("Creating home collector for home %s\n", home.Id)
	return &HomeCollector{
		home:                    home,
		price:                   prometheus.NewDesc("tibber_power_price", "Power price", []string{"type"}, prometheus.Labels{"home_id": string(home.Id)}),
		priceLevel:              prometheus.NewDesc("tibber_power_price_level", "Power price level", nil, prometheus.Labels{"home_id": string(home.Id)}),
		nextHourPrice:           prometheus.NewDesc("tibber_power_price_next_hour", "Power price next hour", []string{"type"}, prometheus.Labels{"home_id": string(home.Id)}),
		nextHourPriceLevel:      prometheus.NewDesc("tibber_power_price_level_next_hour", "Power price level next hour", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousHourConsumption: prometheus.NewDesc("tibber_power_consumption_previous_hour", "Power consumption previous hour", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousHourCost:        prometheus.NewDesc("tibber_power_cost_previous_hour", "Power cost previous hour", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousDayConsumption:  prometheus.NewDesc("tibber_power_consumption_previous_day", "Power consumption previous day", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousDayCost:         prometheus.NewDesc("tibber_power_cost_previous_day", "Power cost previous day", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousHourProduction:  prometheus.NewDesc("tibber_power_production_previous_hour", "Power production previous hour", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousHourProfit:      prometheus.NewDesc("tibber_power_profit_previous_hour", "Power profit previous hour", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousDayProduction:   prometheus.NewDesc("tibber_power_production_previous_day", "Power production previous day", nil, prometheus.Labels{"home_id": string(home.Id)}),
		previousDayProfit:       prometheus.NewDesc("tibber_power_profit_previous_day", "Power profit previous day", nil, prometheus.Labels{"home_id": string(home.Id)}),
	}
}

func (c *HomeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.price
	ch <- c.priceLevel
	ch <- c.nextHourPrice
	ch <- c.nextHourPriceLevel
	ch <- c.previousHourConsumption
	ch <- c.previousHourCost
	ch <- c.previousDayConsumption
	ch <- c.previousDayCost
	ch <- c.previousHourProduction
	ch <- c.previousHourProfit
	ch <- c.previousDayProduction
	ch <- c.previousDayProfit
}

func (c *HomeCollector) Collect(ch chan<- prometheus.Metric) {
	prices := c.home.PricesSnapshot()
	previousHour, previousDay := c.home.PreviousSnapshot()
	if prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Total != nil {
		ch <- prometheus.MustNewConstMetric(
			c.price,
			prometheus.GaugeValue,
			*prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Total,
			"total",
		)
	}
	if prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Energy != nil {
		ch <- prometheus.MustNewConstMetric(
			c.price,
			prometheus.GaugeValue,
			*prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Energy,
			"energy",
		)
	}
	if prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Tax != nil {
		ch <- prometheus.MustNewConstMetric(
			c.price,
			prometheus.GaugeValue,
			*prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Tax,
			"tax",
		)
	}
	if prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Level != nil {
		ch <- prometheus.MustNewConstMetric(
			c.priceLevel,
			prometheus.GaugeValue,
			float64(tibber.PriceLevel[string(*prices.Viewer.Home.CurrentSubscription.PriceInfo.Current.Level)]),
		)
	}
	p := c.home.GetPrices(time.Now().Add(time.Hour))
	if p != nil && len(p) > 0 {
		if p[0].Total != nil {
			ch <- prometheus.MustNewConstMetric(
				c.nextHourPrice,
				prometheus.GaugeValue,
				*p[0].Total,
				"total",
			)
		}
		if p[0].Energy != nil {
			ch <- prometheus.MustNewConstMetric(
				c.nextHourPrice,
				prometheus.GaugeValue,
				*p[0].Energy,
				"energy",
			)
		}
		if p[0].Tax != nil {
			ch <- prometheus.MustNewConstMetric(
				c.nextHourPrice,
				prometheus.GaugeValue,
				*p[0].Tax,
				"tax",
			)
		}
		if p[0].Level != nil {
			ch <- prometheus.MustNewConstMetric(
				c.nextHourPriceLevel,
				prometheus.GaugeValue,
				float64(tibber.PriceLevel[string(*p[0].Level)]),
			)
		}
	}
	if previousHour.Consumption != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousHour.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousHourConsumption,
				prometheus.GaugeValue,
				*previousHour.Consumption,
			),
		)
	}
	if previousHour.Cost != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousHour.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousHourCost,
				prometheus.GaugeValue,
				*previousHour.Cost,
			),
		)
	}
	if previousDay.Consumption != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousDay.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousDayConsumption,
				prometheus.GaugeValue,
				*previousDay.Consumption,
			),
		)
	}
	if previousDay.Cost != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousDay.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousDayCost,
				prometheus.GaugeValue,
				*previousDay.Cost,
			),
		)
	}
	if previousHour.Production != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousHour.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousHourProduction,
				prometheus.GaugeValue,
				*previousHour.Production,
			),
		)
	}
	if previousHour.Profit != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousHour.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousHourProfit,
				prometheus.GaugeValue,
				*previousHour.Profit,
			),
		)
	}
	if previousDay.Production != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousDay.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousDayProduction,
				prometheus.GaugeValue,
				*previousDay.Production,
			),
		)
	}
	if previousDay.Profit != nil {
		ch <- prometheus.NewMetricWithTimestamp(
			previousDay.Timestamp,
			prometheus.MustNewConstMetric(
				c.previousDayProfit,
				prometheus.GaugeValue,
				*previousDay.Profit,
			),
		)
	}
}
