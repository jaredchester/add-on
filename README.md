# CHARLES Hub Home Assistant Add-on

This repository hosts the **CHARLES Hub** add-on. It centralizes the CHARLES persona, routing, feed logging, notifications, and manual triggers behind a simple API and an Ingress UI.

## Install (Home Assistant)
1. In Home Assistant, go to **Settings → Add-ons → Add-on Store → ⋮ → Repositories**.
2. Add this repository URL: `https://github.com/jaredchester/charles_hub`.
3. After it refreshes, the **CHARLES Hub** add-on will appear. Install, enable Start on boot/Watchdog, and Start the add-on.
4. Open the Web UI (Ingress) to edit persona/pools/toggles, test emits, and view the feed.

## Add-on endpoints (internal)
- `POST /api/emit` — router intake: `{topic, category, context, route, title?, notify_service?, notify_tag?, prompt_override?, conversation_id?}`
- `GET/POST /api/state` — read/update settings stored in `/data/state.json` (seeded from add-on options).
- `GET /api/feed` — current feed entries from `/config/www/charles_feed.log`
- `POST /api/mark_read` — clear unread counters/log
- `GET /api/health` — simple status check

The add-on uses the supervisor token to call HA’s conversation agent and notify services. Feed entries are still appended to `/config/www/charles_feed.log` so the existing CRT view keeps working.
