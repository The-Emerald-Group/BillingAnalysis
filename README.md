# General Billing Portal

A lightweight, self-hosted billing portal that aggregates device counts per customer from Ninja and Sophos, caches results in SQLite for fast UI loads, and supports both automatic and manual customer consolidation.

## Current Feature Set

- Scheduled background sync (default once per day) with manual run trigger.
- Detailed sync logging to `stdout` for Docker/Portainer visibility.
- Partial-success sync behavior (one provider can fail while the other still updates cache).
- Searchable customer list with provider icons and status-aware highlighting.
- Status model:
  - Green for matched counts or single-platform customers.
  - Yellow only when both platforms exist and counts mismatch.
  - Explicit "Doesn't have Ninja" / "Doesn't have Sophos" messaging on detail view.
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
  - Counter chips for missing from Sophos, missing from Ninja, and matched.
- Dedicated `/settings` page for admin operations:
  - Manual platform linking (Ninja + Sophos -> canonical name).
  - View and delete platform links.
  - Manual merge mappings.
  - Hide already linked/auto-paired names from picker lists by default.
  - Maintenance actions: dedupe display names and reset merge/cache state.

## Quick Start

1. Configure environment values in `docker-compose.yml`:
   - `NINJA_CLIENT_ID`
   - `NINJA_CLIENT_SECRET`
   - `NINJA_API_BASE` (match your NinjaOne region)
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
- `SYNC_INTERVAL_MINUTES` (default `1440`): Background sync interval.
- `DB_PATH` (default `/data/billing_cache.db`): SQLite cache location.
- `NINJA_CLIENT_ID` (required): NinjaOne API Client ID from Administration → Apps → API → Client App IDs.
- `NINJA_CLIENT_SECRET` (required): NinjaOne API Client Secret (shown once when the app is created).
- `NINJA_API_BASE` (default `https://app.ninjarmm.com`): Regional NinjaOne host (`eu.ninjarmm.com` / `oc.ninjarmm.com` if needed).
- `NINJA_TOKEN_PATH` (default `/ws/oauth/token`): OAuth token endpoint path.
- `NINJA_DEVICES_PATH` (default `/v2/devices-detailed`): Devices list endpoint.
- `NINJA_OAUTH_SCOPE` (default `monitoring`): OAuth scope (Monitoring is enough for read-only counts).
- `SOPHOS_CLIENT_ID` (required): Sophos API client ID.
- `SOPHOS_CLIENT_SECRET` (required): Sophos API client secret.
- `SOPHOS_TOKEN_URL` (default `https://id.sophos.com/api/v2/oauth2/token`): Sophos OAuth token endpoint.
- `RECENT_DEVICE_CUTOFF_DAYS` (default `30`): Base cutoff window (used as initial/default for each provider).
- `SOPHOS_RECENTLY_ONLINE_DAYS` (legacy fallback, default `30`): Used only when `RECENT_DEVICE_CUTOFF_DAYS` is not set.
- `REQUEST_TIMEOUT_SECONDS` (default `30`): HTTP timeout per call.
- `MAX_RETRIES` (default `3`): Retry attempts for provider API calls.
- `RETRY_DELAY_SECONDS` (default `1.5`): Retry backoff multiplier.

## How Sync Works

1. Authenticate to NinjaOne with OAuth client credentials, then fetch devices (`/v2/devices-detailed`).
2. Fetch Sophos partner tenants and endpoint data.
3. Optionally keep only recently online devices using the Settings cutoff toggle(s).
4. Aggregate counts per source customer and apply normalization + explicit links/merges.
5. Upsert current counts, keep history snapshots, and record sync run metadata.
6. If one provider fails, store the successful provider data and mark run as partial.

## API Endpoints

- `GET /api/customers`: Searchable cached customer list (with sort/filter/status metrics).
- `GET /api/customers/<id>`: Detail counts/status for one customer.
- `GET /api/customers/<id>/device-compare`: Device-level comparison for one customer.
- `GET /api/sync/status`: Sync metadata and latest run status.
- `POST /api/sync/run`: Trigger async/manual sync.
- `GET /api/settings`: Read cutoff settings (global + provider-specific).
- `PUT /api/settings`: Update cutoff settings (supports provider-specific fields).

### Cutoff Modes

- Global mode (default): one cutoff toggle + day value applies to both Ninja and Sophos.
- Provider mode: configure Ninja and Sophos cutoff toggle/day values independently.
- Both modes are managed in `/settings` under **Device Cutoff**.
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
- `platform_links`: Explicit Ninja/Sophos canonical links.

## Security Notes

- Keep credentials only in environment variables or Docker secrets.
- Never commit real credentials to source control.
- Rotate Sophos credentials before expiry and rotate NinjaOne client secrets when required.

## Troubleshooting

- **Ninja auth failed**: Confirm `NINJA_CLIENT_ID` / `NINJA_CLIENT_SECRET`, grant type Client Credentials, scope `monitoring`, and the correct regional `NINJA_API_BASE`.
- **Only one provider updates**: Expected when run is partial; inspect logs for provider-specific errors.
- **Merge/link not visible immediately**: Use `/settings` and confirm the link exists in "Current Platform Links".
- **No Sophos devices in compare**: Verify endpoints are recently online (Settings -> Device Cutoff days).
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
