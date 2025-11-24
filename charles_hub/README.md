# CHARLES Hub (Minimal Persona)

Minimal Home Assistant add-on that only stores a persona prompt and writes it to `/share/charles_persona.txt` on start. Service logs the prompt and exits cleanly.

## Options
- `persona_prompt` (string, optional): CHARLES persona text. Defaults to a short prompt.

## Build/Install
- Add this repository to Home Assistant Add-on Store.
- Install/rebuild the add-on. After start, check `/share/charles_persona.txt` for the saved prompt.
