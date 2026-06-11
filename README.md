# maigret-runner

A small self-hosted web wrapper around [Maigret](https://github.com/soxoj/maigret)
that submits a username, runs the scan in the background, stores the
HTML report in S3 (Garage), and serves a tiny dashboard listing past
scans. Cloudflare-protected targets are bounced through
[FlareSolverr](https://github.com/FlareSolverr/FlareSolverr) when a
direct fetch trips the JS challenge.

Built for [apis-mellifera](https://github.com/Siem2l/apis-mellifera)
but works standalone via `docker compose up`.

## What it does

- **POST /** with `username=...` → kicks off a background scan
- **GET /** → list of past scans with status + hit counts
- **GET /scans/{id}** → full HTML report embedded inline (plus the
  JSON sidecar for programmatic consumers)
- **/api/scans, /api/metrics, /healthz** → JSON + Prometheus
  + liveness for ops

The HTML report is whatever Maigret's own `--html` writer produces
(account links per site with proof-of-existence claims), stored in S3
keyed by the job UUID. The SQLite index is just for fast list-view
rendering.

## Configuration

All env-based; defaults assume the apis-mellifera shape (Garage at
`http://garage:3900`, FlareSolverr at `http://flaresolverr:8191/v1`).

| Var | Default | Purpose |
|---|---|---|
| `HOST` / `PORT` | `0.0.0.0` / `8080` | uvicorn bind |
| `PUBLIC_HOST` | `maigret.siem2l.nl` | canonical host shown in templates |
| `DATA_DIR` | `/data` | SQLite jobs index location |
| `S3_ENDPOINT_URL` | `http://garage:3900` | Garage S3 API |
| `S3_BUCKET` | `maigret-reports` | bucket for HTML + JSON |
| `S3_REGION` | `garage` | any non-empty string |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | (required) | bucket creds |
| `FLARESOLVERR_URL` | (none) | enables CF bypass when set |
| `MAIGRET_TOP_SITES` | `500` | cap per scan to keep latency bounded |
| `MAIGRET_TIMEOUT_PER_SITE` | `10` | seconds |

## Local dev

```bash
docker compose up --build
# → http://localhost:8080
```

The compose file spins up MinIO as a stand-in for Garage so you can
poke the whole pipeline without touching production.

## Production (apis-mellifera)

```
services.apis-mellifera.maigret.enable = true;
```

The Nix module pins to a specific tag from this repo. Bump
deliberately:

1. Tag a new version here (`git tag v0.2.0 && git push --tags`).
2. Wait for the `release` workflow to publish
   `ghcr.io/siem2l/maigret-runner:v0.2.0`.
3. Bump the `image` default in `modules/services/osint/maigret.nix`
   and `make deploy-nixos`.

## License

MIT
