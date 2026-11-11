# Weir Helm chart

Deploys the [Weir](https://weirhq.com) server: it receives reports from the Weir SDK and scraper,
answers WeirQL queries, and serves the UI.

```bash
helm install weir oci://ghcr.io/weir-hq/charts/weir --namespace weir --create-namespace
```

Then point your services at it with `WEIR_ENDPOINT=http://weir.weir.svc:4100`, and open the UI:

```bash
kubectl -n weir port-forward svc/weir 4100
```

## Before you use it

- **In memory, one replica.** The server keeps 15 minutes of data in memory. A restart clears it;
  senders refill it within a minute. The chart runs exactly one replica and replaces it on
  upgrade (`Recreate`), because two replicas would each see only part of the reports.
- **No authentication yet.** Keep the service on a private network. If you enable the ingress,
  put authentication in front of it.

## Values

| Value | Default | What |
|---|---|---|
| `image.repository` | `ghcr.io/weir-hq/weir-server` | Server image |
| `image.tag` | chart `appVersion` | |
| `service.type`, `service.port` | `ClusterIP`, `4100` | |
| `ingress.enabled` | `false` | Ingress for the UI and API; set `className`, `hosts`, `tls` |
| `resources` | 50m CPU, 64Mi requested; 512Mi limit | Memory grows with the number of stages |
| `extraArgs`, `extraEnv` | `[]` | Extra flags and environment for `weir-server` |
| `podSecurityContext`, `securityContext` | non-root, read-only root, no capabilities | |
| `serviceAccount.create` | `true` | The server does not use the Kubernetes API; no token is mounted |
| `nodeSelector`, `tolerations`, `affinity`, `priorityClassName` | | Scheduling |

All values are in [values.yaml](values.yaml).

## Grafana

Add a Weir data source with URL `http://weir.weir.svc:4100` (the
[Weir Grafana plugin](https://github.com/weir-hq/weir/tree/main/grafana-plugin)).

## Test

`helm test weir` sends one report to the server and checks that a query returns it.
