# Weir Grafana data source

The Grafana plugin for Weir: WeirQL queries in dashboards, the Node graph for `graph()`, and
alerting. What users see in the plugin catalog is [src/README.md](src/README.md).

- `pkg/`: the backend, in Go. Runs queries against the Weir server and builds data frames.
  It runs in Grafana's server, which is what makes alerting work.
- `src/`: the query and config editors, variable support, and the bundled dashboard.
- `.config/`: build setup from `@grafana/create-plugin`. Don't edit it; update it with
  `npx @grafana/create-plugin@latest update`.

## Develop

Requirements: Node 24.5+, Go, [Mage](https://magefile.org), Docker.

```bash
npm install
npm run build          # frontend into dist/ (npm run dev to watch)
mage -v                # backend binaries into dist/
go test ./pkg/...
npm run typecheck && npm run lint && npm run test:ci

cd ../demo && make up FAULT=quick && cd -   # a Weir server with data on :4100
docker compose up --build                   # Grafana on http://localhost:3000
```

Grafana starts with the Weir data source and the **Weir pipeline** dashboard provisioned.

## Release

The plugin id is `weirhq-weir-datasource`. Its first part must match the grafana.com
organization that signs and publishes it. Sign with `npm run sign` (needs
`GRAFANA_ACCESS_POLICY_TOKEN`), zip `dist/` as `weirhq-weir-datasource/`, then submit it at
grafana.com → My Plugins.
