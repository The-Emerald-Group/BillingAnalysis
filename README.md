# General Billing Portal

A lightweight, self-hosted billing portal that aggregates **device counts per customer** from **N-able** and **Sophos**, stores those counts locally in SQLite, and serves a fast searchable UI from cache.

## Features

- Scheduled background sync (default: every 3 hours)
- Local cache for fast UI load and filtering
- Normalized customer matching between N-able and Sophos (case/spacing/punctuation tolerant)
- Searchable customer list with mini provider icons
- Customer detail cards showing:
  - N-able device count
  - Sophos device count
- Manual sync trigger from the UI
- Sync status visibility (last sync time + health state)

## Quick Start

1. Configure environment values in `docker-compose.yml`:
   - `NABLE_TOKEN`
   - `SOPHOS_CLIENT_ID`
   - `SOPHOS_CLIENT_SECRET`
2. Start the app:

```bash
docker compose up -d --build
```

3. Open:

- [http://localhost:8083](http://localhost:8083)

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8083` | Web server port |
| `SYNC_INTERVAL_MINUTES` | `180` | Background sync interval |
| `DB_PATH` | `/data/billing_cache.db` | SQLite cache location |
| `NABLE_TOKEN` | required | N-able JWT token |
| `NABLE_API_BASE` | `https://api.n-able.com` | N-able API base URL |
| `NABLE_DEVICES_PATH` | `/devices` | N-able devices endpoint path |
| `SOPHOS_CLIENT_ID` | required | Sophos API client ID |
| `SOPHOS_CLIENT_SECRET` | required | Sophos API client secret |
| `SOPHOS_TOKEN_URL` | `https://id.sophos.com/api/v2/oauth2/token` | Sophos OAuth token endpoint |
| `REQUEST_TIMEOUT_SECONDS` | `30` | HTTP timeout per call |
| `MAX_RETRIES` | `3` | Retry attempts for API requests |
| `RETRY_DELAY_SECONDS` | `1.5` | Backoff multiplier between retries |

## How Sync Works

1. Fetch all N-able devices and group by customer name.
2. Fetch Sophos tenants, then fetch endpoint totals per tenant.
3. Normalize customer names and merge cross-platform records.
4. Upsert `latest` counts and append a timestamped history snapshot.
5. Keep serving cached data even if a later sync fails.

## API Endpoints

- `GET /api/customers` - searchable cached customer list
- `GET /api/customers/<id>` - detailed counts for one customer
- `GET /api/sync/status` - sync metadata and latest run status
- `POST /api/sync/run` - trigger a manual sync run

## Data Storage

SQLite tables:

- `customers` - merged identity and source names
- `customer_counts_latest` - current counts used by UI
- `customer_count_history` - historical snapshots
- `sync_runs` - sync run audit and error summaries

## Security Notes

- Keep API credentials in environment variables only.
- Never commit real credentials to source control.
- Rotate Sophos credentials before expiration.

## Troubleshooting

- **No data shown**: verify credentials and run manual sync from the UI.
- **Sync errors**: check container logs with `docker logs billing-portal`.
- **Slow startup**: first sync can take longer because all tenants/customers are fetched.

## Docker CI/CD Workflow

This repo includes `.github/workflows/docker-build.yml` to match your other projects.

- Builds on `main`, tags (`v*`), PRs, and manual dispatch.
- Multi-arch build target: `linux/amd64` and `linux/arm64`.
- Pushes images to GitHub Container Registry (`ghcr.io`) for non-PR runs.

The workflow uses the built-in `GITHUB_TOKEN` (with `packages: write`) and publishes to:

- `ghcr.io/<owner>/<repo>:latest`
- `ghcr.io/<owner>/<repo>:<tag>`
- `ghcr.io/<owner>/<repo>:sha-<commit>`
