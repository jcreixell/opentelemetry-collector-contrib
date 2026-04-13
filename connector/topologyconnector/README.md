# Topology Connector

| Status                   |           |
| ------------------------ | --------- |
| Stability                | [development]: traces_to_metrics |
| Supported pipeline types | traces to metrics |
| Distributions            | [contrib] |

## Overview

The Topology Connector consumes traces and emits a gauge metric for each
discovered service-to-service edge. The metric value is always `1`; its
presence indicates the edge is known.

```
topology_edge{
  source_service_name="service-a",
  source_service_namespace="ns",
  destination_service_name="service-b",
  destination_service_namespace="ns"
} 1
```

Edge endpoint labels use both `service.name` and `service.namespace`, the
[OTel entity identifying attributes][otep-0264] for a `service` entity. This
makes the metric directly joinable with resource-attributed metrics from
Prometheus or OTLP pipelines.

### How it works

Edges are discovered by matching `client`/`server` (or `producer`/`consumer`)
span pairs within the same trace — the same mechanism used by
[`servicegraphconnector`](../servicegraphconnector/). Once both sides of a
span pair are observed, the edge is recorded as confirmed.

The connector re-emits the full set of known edges every `emit_interval` so
that the downstream metric store always has fresh data points.

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

extensions:
  file_storage:
    directory: /var/lib/otelcol/topology
```

## Relationship to `servicegraphconnector`

The Topology Connector is intentionally simpler than `servicegraphconnector`:

| | `servicegraphconnector` | `topologyconnector` |
|---|---|---|
| Output | Request counts, latency histograms, error rates | Edge presence only (`topology_edge=1`) |
| Edge identity | `service.name` only | `service.name` + `service.namespace` |
| Persistence | None (in-memory) | Optional via storage extension |
| Edge lifetime | Minutes (in-memory cache) | Configurable, default 7 days |

Use `servicegraphconnector` for RED metrics; use `topologyconnector` for
entity graph construction and service dependency maps.

[otep-0264]: https://github.com/open-telemetry/oteps/blob/main/text/entities/0264-resource-and-entities.md
[development]: https://github.com/open-telemetry/opentelemetry-collector#development
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
