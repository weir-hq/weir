# Weir data source for Grafana

[Weir](https://weirhq.com) tracks every item through async pipelines: queues, batches, workflows,
ETLs and agent pipelines. This data source runs WeirQL queries against a Weir server, so you can
chart and alert on:

- **Stuck or late items:** the age of the oldest item in flight, per stage.
- **Lost items:** items that vanished or errored, and conservation per stage
  (`in = out + dropped + in_flight`).
- **Where time goes:** waiting before each stage versus time inside it.
- **What is waiting between stages:** items in a topic or staging table, and how old the oldest is.

## Requirements

- Grafana 13.2 or later.
- A running Weir server: see [the Weir README](https://github.com/weir-hq/weir) (`make up` in `demo/` runs one with a demo pipeline).
  The server has no authentication yet: keep it on a private network.

## Set up

1. Install the plugin: **Administration → Plugins and data → Plugins**, search for **Weir**.
2. Add a data source of type **Weir** and set **Weir server URL**, for example
   `http://weir-server:4100`. Grafana's server makes the requests, so use an address it can reach.
3. **Save & test** shows how many query functions the server offers.

Or provision it:

```yaml
apiVersion: 1
datasources:
  - name: Weir
    type: weirhq-weir-datasource
    access: proxy
    url: http://weir-server:4100
```

The plugin includes a **Weir pipeline** dashboard. Import it from the data source's
**Dashboards** tab.

## Queries

```
function( {label="value", label=~"regex", label!="x"} [range] [, param] ) [by (label, ...)]
```

| Query                                                 | Shows                                                                          |
| ----------------------------------------------------- | ------------------------------------------------------------------------------ |
| `oldest({pipeline="orders"})`                         | Age of the oldest item still in each stage. Climbs when something is stuck.    |
| `lost({pipeline="orders"})`                           | Items vanished or errored per second.                                          |
| `throughput({pipeline="orders"})`                     | Items entering each stage per second.                                          |
| `age({pipeline="orders"}, 0.99)`                      | Item age when it leaves each stage, at a quantile.                             |
| `in_flight({pipeline="orders"})`                      | Items received but not closed.                                                 |
| `waiting({pipeline="orders"})`, `oldest_waiting(...)` | Items waiting on each edge, and the oldest one's age (needs the Weir scraper). |
| `graph({pipeline="orders"}[5m])`                      | Stages and edges. Use the **Node graph** panel.                                |
| `leak({pipeline="orders"}[15m])`                      | Conservation per stage, as a table.                                            |
| `drops({pipeline="orders"}[1h]) by (reason)`          | Why items did not pass.                                                        |
| `decompose({pipeline="orders"}[15m])`                 | Where end-to-end age comes from.                                               |
| `transit({pipeline="orders"}[15m])`                   | Per edge: items waiting, the oldest one's age, items lost in between.          |

Labels are `pipeline` and `stage`. Series functions follow the dashboard time range. Table and graph
functions use `[range]` when given, and the dashboard time range otherwise.

Time series carry `pipeline` and `stage` labels (and `upstream` for edge series), so alert rules
fire one alert per stage.

## Variables

Use a **Query** variable with:

- `label_values(pipeline)`
- `label_values(stage, pipeline="$pipeline")`

In queries, a variable with several values becomes a regex alternation: use it with `=~`, as in
`oldest({stage=~"$stage"})`.

## Alerting

Alert on any time series query. Two useful rules:

- Stuck items: `oldest({pipeline="orders"})`, reduce with **Last**, threshold above `300` (seconds).
- Lost items: `lost({pipeline="orders"})`, reduce with **Max**, threshold above `0`.

## Links

- [Weir on GitHub](https://github.com/weir-hq/weir)
- [Report an issue](https://github.com/weir-hq/weir/issues)
