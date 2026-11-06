# webrtc-transfer-app

Trasferimento file P2P browser-to-browser via **WebRTC DataChannel** (cifrato DTLS), come Mistral App.

- `apps/web/public/` — UI statica (`index.html`, `style.css`, `app.js`), servita a `/` (la route `/` reindirizza a `/index.html`)
- `apps/api` — FastAPI; espone `/api/health` e il WebSocket di segnalazione `/api/ws` (`apps/api/src/api/routers/api/ws/route.py`): stanze di due peer che si scambiano SDP/ICE. I contenuti dei file non passano mai dal server

## Funzionamento

1. Chi invia genera un **codice invito** (segnale manuale, copia-incolla — funziona senza alcun server) oppure crea una **stanza** con codice a 5 lettere sulla modalità online, che usa `/api/ws`
2. Chi riceve incolla il codice / si unisce alla stanza: la connessione WebRTC si apre
3. I file vengono inviati a chunk da 16 KB con barra di progresso e velocità; chi riceve li scarica automaticamente

## Sviluppo locale

```bash
mistral apps dev --no-open          # stack locale (web :3001, api :3000, gateway :8080)
bunx nx run web:build               # build di produzione del frontend
uv run --all-packages --with pytest pytest apps/api/tests -q   # test API
```

## Deploy

```bash
mistral deploy --from-disk --name webrtc-transfer-app --json
```

Poi pubblicazione nel catalogo Vibe con lo strumento MCP `publish_app` (ambiente `production`).
