# Tibber exporter

> **Fork note.** This is a fork of [terjesannum/tibber-exporter](https://github.com/terjesannum/tibber-exporter)
> that adds **15-minute (quarter-hourly) electricity price resolution**: `UpdatePrices`
> queries `priceInfo(resolution: QUARTER_HOURLY)` and transparently falls back to the
> upstream hourly query if a home/market doesn't support it. It also keeps running when
> the live feed fails, instead of exiting (see [Live feed](#live-feed)).
> Images are published to `ghcr.io/yornik/tibber-exporter` with semver tags (CI in
> `.github/workflows/ci.yaml`); the upstream chart/binary release pipeline is dropped.
> Everything else tracks upstream — please send general fixes there.

Monitor your power usage and costs with Prometheus and Grafana.

![Grafana dashboard](grafana/dashboard.png)

## Description

This prometheus exporter will connect to the Tibber API, subscribe to updates from [Tibber Pulse](https://tibber.com/no/pulse) or [Watty](https://tibber.com/se/store/produkt/watty-tibber) and make the metrics available for Prometheus.

See the provided [Grafana dashboard](grafana/dashboard.json) for examples on how they can be used.  
Note that the consumption, cost and price heatmap panels requires the [Hourly heatmap plugin](https://grafana.com/grafana/plugins/marcusolsson-hourly-heatmap-panel/).

#### Don't have Tibber Pulse or Watty?

If you don't have a device for live measurements, only the power price metrics will be available on that dashboard. It is possible to create a dashboard with historic consumption and cost using the `..._previous_day` and `..._previous_hour` metrics, but the availability of those metrics may vary between grid companies. See the [dashboard without pulse](grafana/dashboard-without-pulse.json) for an example using the previous day metrics:

![Grafana dashboard without pulse](grafana/dashboard-without-pulse.png)

These metrics can also be used for a [monthly overview dashboard](grafana/dashboard-monthly.json):

![Grafana monthly overview dashboard](grafana/dashboard-monthly.png)

#### Next day prices

The Tibber API provides power prices for the current and the next day, but storing future data can't be done easily in Prometheus. To be able to show future power prices, this exporter also has a JSON service which can be queried to get this data. In Grafana this can be used with the [Infinity datasource plugin](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/).

| Endpoint                                  | Description                                               |
|-------------------------------------------|-----------------------------------------------------------|
| `/homes`                                  | Available homes                                           |
| `/homes/<home id>/prices`                 | Power prices for today and tomorrow (if available)        |
| `/homes/<home id>/prices?period=now`      | Power prices from now and through tomorrow (if available) |
| `/homes/<home id>/prices?period=today`    | Power prices for today                                    |
| `/homes/<home id>/prices?period=tomorrow` | Power prices for tomorrow (if available)                  |

Next day prices are usually available around 13:00 each day.

See the [prices dashboard](grafana/dashboard-prices.json) for some example panels which use this datasource.

![Grafana prices dashboard](grafana/dashboard-prices.png)

## Running

Docker image is available on [ghcr.io](https://github.com/terjesannum/tibber-exporter/pkgs/container/tibber-exporter).  
Prebuilt binaries for Linux, MacOS and Windows are available on the [release page](https://github.com/terjesannum/tibber-exporter/releases/latest).

A `TIBBER_TOKEN` is required to use the Tibber API, go to [developer.tibber.com](https://developer.tibber.com/) to find yours.

Run it with an automatic restart mechanism anyway: the exporter still exits on configuration errors, such as a port that is in use or a `--live` home that is not found.

There is a bug in the Tibber service which sometimes fails to report that a home has the live feed feature. To handle this situation, the exporter has a command line option `--live` that can be used to force the exporter to always start the live feed. Therefore, if you have the live feature for your home, it's recommended to run the exporter with `--live <home id>` or set the environment variable `TIBBER_LIVE_ENABLE_HOMES=<home id>` to ensure that the live feed is always started.

### Live feed

The exporter does not exit when the live feed fails. Each websocket session to Tibber ends sooner or later: Tibber restarts its servers, goes silent on a connection that is 24 hours old, or refuses to start the stream for a while. The exporter then opens a new session in the same process, so the price and previous hour/day metrics keep coming and nothing is lost to a restart.

- Before each new session it waits a random time between half and all of a delay that starts at 5 seconds and doubles up to 5 minutes. After a session that delivered data for 10 minutes, the delay starts at 5 seconds again. A long Tibber outage costs about one attempt every 2.5 to 5 minutes.
- `--live-feed-timeout` (minutes, default 1, values below 1 count as 1) is how long a session may go without a new measurement. Then the session is closed and a new one started. Keep-alives, errors without an end of the subscription and a repeated measurement do not count as new. Tibber sends a measurement about every 10 seconds, so the default notices a dead feed within a minute.
- The exporter closes a session after 23 hours 55 minutes and starts a new one, before Tibber's 24 hour limit. It waits until the old connection is closed before it opens the next one, so it never holds two connections for one token.
- A measurement that arrives with an older timestamp than the stored one is skipped, so the exported timestamps never go backwards.
- The live gauges are not exported once the last measurement is more than 5 minutes old, so a dead feed shows an old value for at most 5 minutes. `tibber_live_connected` is 1 while a session is open and has delivered new data. It drops to 0 when the session ends, at the latest `--live-feed-timeout` after the last new measurement.
- The log has one line for each session end, with the reason and, if there is one, the error behind it. It does not log each measurement.

If the homes query fails at start-up (Tibber answers 502 or 504 now and then), the exporter retries it with the same delays. The HTTP listener starts before the first call to Tibber, so `/healthz` and `/metrics` answer while Tibber is slow or down. Every HTTP call to Tibber, the websocket handshake included, has a 30 second timeout.

On SIGTERM or SIGINT the exporter ends the subscription, closes the websocket with a close frame and exits with status 0. A second signal ends it at once.

The exporter sends the API token in an `Authorization` header only to `api.tibber.com` and `websocket-api.tibber.com`. The live feed also sends it in its `connection_init` message, which is how Tibber authenticates the websocket, so a `--live-url` on another host gets the token there and not in a header.

#### Health checks and alerts

`/healthz` answers `ok` as soon as the HTTP listener runs. Use it for both the liveness and the readiness probe. It does not depend on Tibber, on purpose: restarting the exporter or taking it out of service does not fix a Tibber outage, and a probe that fails during one turns it into a restart loop or a pod-not-ready alert. Watch Tibber with the metrics instead, for example:

```yaml
# A feed that flaps (short sessions with long pauses between them) never
# stays at 0 for long, so look at the share of time it was connected.
- alert: TibberLiveFeedDown
  expr: max by (home_id) (avg_over_time(tibber_live_connected[30m])) < 0.5
  for: 10m
- alert: TibberLiveDataStale
  expr: time() - max by (home_id) (tibber_live_last_message_timestamp_seconds) > 600
  for: 5m
- alert: TibberHomesNotLoaded
  expr: absent(tibber_home_info)
  for: 15m
```

`time() - tibber_live_last_message_timestamp_seconds` is the age of the live data, and `increase(tibber_live_reconnects_total[1h])` shows why sessions ended.

### Docker compose

If you don't already run Grafana and Prometheus, you can try a complete setup with `docker compose`.

```sh
cd docker-compose
TIBBER_TOKEN=... docker compose up
```

Then go to http://localhost:3000/ and find the dashboards from the menu.

### Kubernetes

Install in your kubernetes cluster with [Helm](https://helm.sh/). First add the the helm repository:

```sh
helm repo add tibber-exporter https://terjesannum.github.io/tibber-exporter/
helm repo update
```

Then install the helm chart:

```sh
helm install tibber-exporter tibber-exporter/tibber-exporter --set tibberToken=...
```

This with install the exporter with the `prometheus.io/scrape` annotation set to `true`. If you run the [Prometheus operator](https://github.com/prometheus-operator/prometheus-operator), install with `serviceMonitor.enabled=true` to create a `ServiceMonitor` instead:

```sh
helm install tibber-exporter tibber-exporter/tibber-exporter --set tibberToken=... --set serviceMonitor.enabled=true
```

### Docker container

```sh
docker run -d -p 8080:8080 -e TIBBER_TOKEN=... --restart always ghcr.io/terjesannum/tibber-exporter:latest
```

### Binary

Run with your Tibber token in the `TIBBER_TOKEN` environment variable, or specify it with the `-token` flag:

```sh
tibber-exporter -token ...
```

## Prometheus

Prometheus need to be configured to scrape the exporter, so add a scrape job to `/etc/prometheus/prometheus.yml`:

```yaml
scrape_configs:

  - job_name: "tibber-exporter"
    scrape_interval: 5s
    static_configs:
      - targets: ["localhost:8080"]
```

How often the data is updated depends on your energy meter. Look at the logs from the exporter to see how often it receives updates, and adjust the scrape interval. Shorter scrape interval generates more data, so consider scraping less frequent if you only use a dashboard with a wide time range and don't need "live" updates.

Also remember that Prometheus is designed for monitoring and not precise calculation, so don't expect the result of the queries to excactly match your electricity bill.

## Grafana

Import the [dashboard](grafana/dashboard.json) or use id `16804` and import from [grafana.com](https://grafana.com/grafana/dashboards/16804-tibber/). Then select the Prometheus datasource that scrapes the exporter.

## Metrics

| Name                                     | Description                                          | Type    |
|------------------------------------------|------------------------------------------------------|---------|
| tibber_home_info                         | Home info                                            | Gauge   |
| tibber_grid_info                         | Power grid info                                      | Gauge   |
| tibber_power_price                       | Current power price                                  | Gauge   |
| tibber_power_price_level                 | Current price level                                  | Gauge   |
| tibber_power_price_next_hour             | Power price next hour                                | Gauge   |
| tibber_power_price_level_next_hour       | Price level next hour                                | Gauge   |
| tibber_power_consumption                 | Current power consumption                            | Gauge   |
| tibber_power_consumption_day_avg         | Average power consumption today                      | Gauge   |
| tibber_power_consumption_day_min         | Minimum power consumption today                      | Gauge   |
| tibber_power_consumption_day_max         | Maximum power consumption today                      | Gauge   |
| tibber_power_consumption_day_total       | Total power consumption today                        | Counter |
| tibber_power_consumption_previous_hour   | Total power consumption previous hour                | Gauge   |
| tibber_power_consumption_previous_day    | Total power consumption yesterday                    | Gauge   |
| tibber_power_consumption_reactive        | Current reactive consumption                         | Gauge   |
| tibber_power_consumption_meter_total     | Last consumption meter state                         | Counter |
| tibber_power_cost_day                    | Total power cost today                               | Gauge   |
| tibber_power_cost_day_total              | Total power cost today                               | Counter |
| tibber_power_cost_previous_hour          | Total power cost previous hour                       | Gauge   |
| tibber_power_cost_previous_day           | Total power cost yesterday                           | Gauge   |
| tibber_power_production                  | Current power production                             | Gauge   |
| tibber_power_production_day_min          | Minimum power production today                       | Gauge   |
| tibber_power_production_day_max          | Maximum power production today                       | Gauge   |
| tibber_power_production_day_total        | Total power production today                         | Counter |
| tibber_power_production_previous_hour    | Total power production previous hour                 | Gauge   |
| tibber_power_production_previous_day     | Total power production yesterday                     | Gauge   |
| tibber_power_production_reward_day       | Total power production reward today                  | Gauge   |
| tibber_power_production_reward_day_total | Total power production reward today                  | Counter |
| tibber_power_production_reactive         | Current net reactive production                      | Gauge   |
| tibber_power_production_meter_total      | Last production meter state                          | Counter |
| tibber_power_factor                      | Current power factor (active power / apparent power) | Gauge   |
| tibber_power_profit_previous_hour        | Total power profit previous hour                     | Gauge   |
| tibber_power_profit_previous_day         | Total power profit yesterday                         | Gauge   |
| tibber_voltage                           | Phase voltage                                        | Gauge   |
| tibber_current                           | Line current                                         | Gauge   |
| tibber_signal_strength                   | Tibber Pulse signal strength                         | Gauge   |
| tibber_exporter_build_info               | Tibber exporter build and version info               | Gauge   |
| tibber_live_connected                    | 1 while a live feed session is open and has delivered new data, else 0 | Gauge   |
| tibber_live_last_message_timestamp_seconds | Time the last new live measurement was received    | Gauge   |
| tibber_live_reconnects_total             | Live feed sessions that ended, by `reason`           | Counter |

The `reason` label of `tibber_live_reconnects_total` is one of `read_timeout` (no new measurement for `--live-feed-timeout`, also Tibber's silent cut at 24 hours), `graphql_error` (Tibber sent an error such as "unable to start stream", and no data came after it), `complete` (Tibber ended the subscription without an error), `server_close` (a close frame, for example 1001 "Server restart"), `connection_lost` (EOF or connection reset), `dial_failed` (connect, TLS or websocket handshake failed) and `max_age` (closed by the exporter before the 24 hour limit). Every reason starts at 0, so `increase()` also counts the first one. All the `tibber_live_` metrics have the `home_id` label, like the other metrics.

More information can be found in the [Tibber API documentation](https://developer.tibber.com/docs/reference).

## Consider becoming a Tibber customer?

I would be happy if you use [this referral code](https://invite.tibber.com/754c625a). That will give each of us a bonus to use on stuff like Tibber Pulse :smile:

Or register the code `754c625a` directly in the app if you signed up without an invite.
