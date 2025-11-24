# CHARLES Hub (API Stub)

Minimal API add-on to hold CHARLES persona and routing state. Phase 1 provides `/api/state`, `/api/health`, `/api/feed`, `/api/emit` (stub), and `/api/mark_read`. State persists to `/data/state.json` and is seeded from add-on options.

## Options
- `persona_prompt` (string): CHARLES persona text.

## Endpoints
- `GET /api/health` — status.
- `GET/POST /api/state` — read/update stored state.
- `GET /api/feed` — read `/config/www/charles_feed.log`.
- `POST /api/emit` — stub emit (Phase 2 will call HA).
- `POST /api/mark_read` — clears unread counters/logs.

## Build/Install
- Add this repository to Home Assistant Add-on Store.
- Install/rebuild the add-on. The service runs on port 8099 (by default) inside the add-on network.
