# Billing Analysis

This repository contains the **General Billing Portal** project in `general-billing-portal/`.

## Included Project

- `general-billing-portal/` - Dockerized Python + HTML app that:
  - syncs N-able and Sophos device counts on a schedule
  - stores counts in local SQLite cache
  - provides a fast searchable customer UI with provider icons/cards

## Quick Start

```bash
cd general-billing-portal
docker compose up -d --build
```

Then open [http://localhost:8083](http://localhost:8083).

For full setup/configuration details, see `general-billing-portal/README.md`.
