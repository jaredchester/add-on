# CHARLES Hub (API)

API add-on to hold CHARLES persona and routing state. Includes `/api/state`, `/api/health`, `/api/feed`, `/api/emit` (with feed/notify), and `/api/mark_read`. State persists to `/data/state.json` and is seeded from add-on options.

## Options
- `persona_prompt` (string): CHARLES persona text.

## Endpoints
- `GET /api/health` — status.
- `GET/POST /api/state` — read/update stored state.
- `GET /api/feed` — read `/config/www/charles_feed.log`.
- `POST /api/emit` — generates reply via HA conversation agent, appends to feed, optionally notifies (respects feed/notify category toggles and throttle).
- `POST /api/mark_read` — clears unread counters/logs.

## Build/Install
- Add this repository to Home Assistant Add-on Store.
- Install/rebuild the add-on. The service listens on port 8099 (host port also mapped to 8099) and via ingress.
