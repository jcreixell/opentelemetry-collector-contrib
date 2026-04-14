# Topology Connector

| Status                   |           |
| ------------------------ | --------- |
| Stability                | [development]: traces_to_metrics, traces_to_logs |
| Supported pipeline types | traces to metrics, traces to logs |
| Distributions            | [contrib] |

## Overview

The Topology Connector consumes traces and produces two kinds of output:

- **Metrics** (`traces_to_metrics`): a gauge metric for each discovered
  service-to-service edge. The value is always `1`; its presence indicates the
  edge is active.
- **Lifecycle events** (`traces_to_logs`): structured log records emitted when
  an edge is first discovered or when it expires due to TTL.

Edges are discovered by matching `client`/`server` (or `producer`/`consumer`)
span pairs within the same trace — the same mechanism used by
[`servicegraphconnector`](../servicegraphconnector/).

### Metric output

```
topology_edge{
  source_service_name="service-a",
  source_service_namespace="ns",
  destination_service_name="service-b",
  destination_service_namespace="ns"
} 1
```

Additional labels can be added via the `dimensions` config (see below).

The connector re-emits the full set of known edges every `emit_interval` so
that the downstream metric store always has fresh data points.

### Lifecycle log events

When `traces_to_logs` is configured, the connector emits a `plog.LogRecord`
for each edge state change:

| `event.name` | When |
|---|---|
| `topology.edge.discovered` | First time a span pair confirms a new edge |
| `topology.edge.expired` | An edge is evicted because it exceeded `edge_ttl` |

Each log record carries the same service/namespace/dimension attributes as the
metric. The body is the event name string.

### Persistence and stickiness

Under head-based sampling, low-traffic connections may only be observed
infrequently. The connector addresses this by:

1. **Persistent storage**: when a `storage` extension (e.g. `file_storage`) is
   configured, discovered edges are saved to disk on shutdown and reloaded on
   startup. A Kubernetes StatefulSet with a PVC is the natural deployment.

2. **Edge TTL**: edges that have not been confirmed by any trace within
   `edge_ttl` (default 7 days) are evicted. This allows the topology to heal
   when services are decommissioned.

## Configuration

```yaml
connectors:
  topology:
    storage: file_storage           # optional; omit for in-memory only
    edge_ttl: 168h                  # how long to retain edges without re-confirmation
    emit_interval: 60s              # how often to flush all known edges as metrics
    store:
      max_items: 1000               # max in-flight span pairs being correlated
      ttl: 2s                       # how long to wait for the matching span
    dimensions:                     # optional extra labels sourced from client resource attrs
      - name: deployment.environment
        default: unknown

extensions:
  file_storage:
    directory: /var/lib/otelcol/topology
```

### `dimensions`

Each entry in `dimensions` names a resource attribute to capture from the
**client** span's resource. The value is added as a metric label and as an
attribute on lifecycle log records.

If the attribute is absent on a given span, the configured `default` value is
used (empty string if `default` is omitted). Dimensions are evaluated in config
order; the order is preserved in the metric and log output.

Example — tag edges by deployment environment:

```yaml
connectors:
  topology:
    dimensions:
      - name: deployment.environment
        default: unknown
```

This produces:

```
topology_edge{
  source_service_name="checkout",
  source_service_namespace="shop",
  destination_service_name="payment",
  destination_service_namespace="shop",
  deployment.environment="production"
} 1
```

## Relationship to `servicegraphconnector`

| | `servicegraphconnector` | `topologyconnector` |
|---|---|---|
| Output | Request counts, latency histograms, error rates, virtual node edges | Edge presence gauge + lifecycle log events |
| Edge identity | `service.name` + configurable `dimensions` (prefixed `client_`/`server_`) | `service.name` + `service.namespace` + configurable dimensions |
| Persistence | None (in-memory only) | Optional via storage extension |
| Edge lifetime | Configurable (default 2s TTL in-flight store; ~15 min metric series cache) | Configurable, default 7 days |
| Virtual nodes | Yes (uninstrumented upstreams via `peer.service`, `db.name`, etc.) | No |
| Messaging spans | `producer`/`consumer` → `MessagingSystem` connection type | `producer`/`consumer` treated same as `client`/`server` |

Use `servicegraphconnector` for RED metrics and virtual node detection; use
`topologyconnector` for long-lived entity graph construction and service
dependency maps with namespace-aware edge identity.

[otep-0264]: https://github.com/open-telemetry/oteps/blob/main/text/entities/0264-resource-and-entities.md
[development]: https://github.com/open-telemetry/opentelemetry-collector#development
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
