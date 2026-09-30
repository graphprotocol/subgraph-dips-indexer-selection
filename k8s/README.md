# Kubernetes Deployment

Kubernetes manifests for the IISA (Indexing Indexer Selection Algorithm) service
and its daily score-computation job.

## Files

- `score-computation-cronjob.yaml` - the namespace plus the daily CronJob that computes indexer scores
- `deployment.yaml` - the IISA HTTP service, with health probes and resource limits
- `service.yaml` - internal ClusterIP service on port 8080
- `iisa-scores-cache-pvc.yaml` - the service's own cache volume for pushed scores and sync status
- `networkpolicy.yaml` - restricts ingress to dipper and the score-computation job
- `kustomization.yaml` - bundles the manifests above for `kubectl apply -k`

## How it fits together

The CronJob runs once a day. It replays gateway query data from Redpanda and reads
indexer stake from the Graph Network subgraph, computes each indexer's scores, and
POSTs them to the IISA service. It pushes an indexer sync-status snapshot the same
way. Nothing is shared on disk between the two.

The IISA service answers indexer-selection requests. On startup it loads the last
scores and sync status from its own cache volume, so a restart serves the most
recent data straight away. Each push atomically rewrites that cache and swaps the
in-memory snapshot the service selects from.

## Resource requirements

The score computation replays up to four weeks of query data, so the CronJob asks
for a lot of memory; the always-on service is comparatively small.

| Workload | CPU (request / limit) | Memory (request / limit) |
|----------|-----------------------|--------------------------|
| IISA service (deployment) | 500m / 4 | 1Gi / 2Gi |
| Score-computation job | 2 / 8 | 50Gi / 50Gi |

## Startup behaviour

The service reads its cache file on startup, which is quick, but the probes leave
generous headroom:

- Startup probe: 30s initial delay, up to 5 minutes total
- Readiness probe: 90s initial delay
- Liveness probe: 120s initial delay

## Prerequisites

These secrets are created out of band and are never committed:

- `iisa-redpanda-credentials` - Redpanda bootstrap servers, SASL username and
  password, and the Graph Network subgraph URL (the URL embeds an API key).
- `iisa-push-token` - the bearer token the CronJob uses to authenticate its pushes;
  the service rejects pushes without it.
- `github-registry-secret` - image pull secret for the GitHub container registry.

The CronJob also needs the `iisa-redpanda-config` ConfigMap, which each environment
supplies to choose what the job scores: `REDPANDA_TOPIC` (for example
`gateway_queries_testnet`) and `REDPANDA_GATEWAY_IDS`. If the ConfigMap or its
`REDPANDA_TOPIC` key is missing, the run fails with `REDPANDA_TOPIC is required` rather
than falling back to the mainnet `gateway_queries` topic.

Geo-location data uses MaxMind GeoLite2, downloaded into the CronJob image at build
time, so no geo API key is needed at runtime.

## Network policy

Only pods labelled `app: dipper` (the consumer) or `app: iisa-score-computation`
(the daily job) may reach the service on port 8080; all other ingress is denied.

## Service discovery

dipper reaches the service through Kubernetes DNS at:

```
http://iisa:8080
```
