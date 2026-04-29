# General Billing Portal

A lightweight, self-hosted billing portal that aggregates device counts per customer from N-able and Sophos, caches results in SQLite for fast UI loads, and supports both automatic and manual customer consolidation.

## Current Feature Set

- Scheduled background sync (default every 3 hours) with manual run trigger.
- Detailed sync logging to `stdout` for Docker/Portainer visibility.
- Partial-success sync behavior (one provider can fail while the other still updates cache).
- Searchable customer list with provider icons and status-aware highlighting.
- Status model:
  - Green for matched counts or single-platform customers.
  - Yellow only when both platforms exist and counts mismatch.
  - Explicit "Doesn't have N-able" / "Doesn't have Sophos" messaging on detail view.
- Sidebar controls for search, status filter, and sorting:
  - Name (A-Z / Z-A)
  - Mismatch delta (high/low)
  - Total devices (high/low)
  - Average total (high/low)
- Summary chips showing matched/mismatched totals with filtered vs total context.
- Device comparison view (per customer):
  - Full merged list of device names from both platforms.
  - Mismatches first, clear "missing from" labels, color-coded rows.
  - Sticky comparison table headers.
  - Counter chips for missing from Sophos, missing from N-able, and matched.
- Dedicated `/settings` page for admin operations:
  - Manual platform linking (N-able + Sophos -> canonical name).
  - View and delete platform links.
  - Manual merge mappings.
  - Hide already linked/auto-paired names from picker lists by default.
  - Maintenance actions: dedupe display names and reset merge/cache state.

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
   - Dashboard: [http://localhost:8083](http://localhost:8083)
   - Settings: [http://localhost:8083/settings](http://localhost:8083/settings)

## Configuration

- `PORT` (default `8083`): Web server port.
- `SYNC_INTERVAL_MINUTES` (default `180`): Background sync interval.
- `DB_PATH` (default `/data/billing_cache.db`): SQLite cache location.
- `NABLE_TOKEN` (required): N-able JWT token.
- `NABLE_API_BASE` (default `https://ncod153.n-able.com`): N-able API host.
- `NABLE_AUTH_PATH` (default `/api/auth/authenticate`): N-able authenticate path.
- `NABLE_DEVICES_PATH` (default `/api/devices`): N-able devices endpoint path.
- `SOPHOS_CLIENT_ID` (required): Sophos API client ID.
- `SOPHOS_CLIENT_SECRET` (required): Sophos API client secret.
- `SOPHOS_TOKEN_URL` (default `https://id.sophos.com/api/v2/oauth2/token`): Sophos OAuth token endpoint.
- `SOPHOS_RECENTLY_ONLINE_DAYS` (default `30`): Include only Sophos endpoints seen within this window.
- `REQUEST_TIMEOUT_SECONDS` (default `30`): HTTP timeout per call.
- `MAX_RETRIES` (default `3`): Retry attempts for provider API calls.
- `RETRY_DELAY_SECONDS` (default `1.5`): Retry backoff multiplier.

## How Sync Works

1. Fetch N-able devices with the same authenticate-and-fetch flow used by the export tool.
2. Fetch Sophos partner tenants and endpoint data.
3. Keep only Sophos endpoints recently online (`lastSeenAt` cutoff).
4. Aggregate counts per source customer and apply normalization + explicit links/merges.
5. Upsert current counts, keep history snapshots, and record sync run metadata.
6. If one provider fails, store the successful provider data and mark run as partial.

## API Endpoints

- `GET /api/customers`: Searchable cached customer list (with sort/filter/status metrics).
- `GET /api/customers/<id>`: Detail counts/status for one customer.
- `GET /api/customers/<id>/device-compare`: Device-level comparison for one customer.
- `GET /api/sync/status`: Sync metadata and latest run status.
- `POST /api/sync/run`: Trigger async/manual sync.
- `GET /api/merge-mappings`: List manual merge mappings.
- `POST /api/merge-mappings`: Create manual merge mapping.
- `GET /api/platform-options`: Candidate names for manual linking.
- `GET /api/platform-links`: List explicit platform links.
- `POST /api/platform-links`: Create explicit platform link.
- `DELETE /api/platform-links/<id>`: Remove explicit platform link.
- `POST /api/maintenance/dedupe-display-names`: Cleanup duplicate display names.
- `POST /api/maintenance/reset-merges`: Purge merge/link state and rebuild cache.

## Data Storage

SQLite tables:

- `customers`: Canonical customer identities and source names.
- `customer_counts_latest`: Latest counts used by dashboard.
- `customer_count_history`: Historical count snapshots.
- `sync_runs`: Sync audit trail and error/partial summaries.
- `merge_mappings`: Manual merge overrides.
- `platform_links`: Explicit N-able/Sophos canonical links.

## Security Notes

- Keep credentials only in environment variables or Docker secrets.
- Never commit real credentials to source control.
- Rotate Sophos credentials before expiry and rotate N-able token when required.

## Troubleshooting

- **N-able 404 on auth**: Check `NABLE_API_BASE` and `NABLE_AUTH_PATH` are not duplicating `/api`.
- **Only one provider updates**: Expected when run is partial; inspect logs for provider-specific errors.
- **Merge/link not visible immediately**: Use `/settings` and confirm the link exists in "Current Platform Links".
- **No Sophos devices in compare**: Verify endpoints are recently online (`SOPHOS_RECENTLY_ONLINE_DAYS`).
- **No data shown**: Verify credentials and run manual sync.
- **Logs**: `docker logs billing-portal` (or container name used in your stack).

## Docker CI/CD Workflow

This repo includes `.github/workflows/docker-build.yml`.

- Builds on `main`, tags (`v*`), PRs, and manual dispatch.
- Multi-arch build target: `linux/amd64` and `linux/arm64`.
- Pushes images to GitHub Container Registry (`ghcr.io`) for non-PR runs.
- Uses built-in `GITHUB_TOKEN` with `packages: write`.

Published images:

- `ghcr.io/<owner>/<repo>:latest`
- `ghcr.io/<owner>/<repo>:<tag>`
- `ghcr.io/<owner>/<repo>:sha-<commit>`
