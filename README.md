# NetworkStats

**NetworkStats** is a lightweight, self-hosted, 24/7 network monitoring and visual traceroute web application—a dark-mode, web-based clone of **PingPlotter**—built with FastAPI, `mtr`, SQLite, TailwindCSS, and Chart.js.

## Features

- **Continuous MTR Hop Engine:** Probes every intermediate hop along the route to each target at a configurable interval (default: `2.0s`), tracking `Hop #`, `IP Address`, `RDNS Hostname`, `Loss %`, `Sent`, `Last`, `Avg`, `Best`, `Worst`, and `Jitter (StDev)`.
- **Pre-Loaded Default Targets:** Automatically monitors `10.0.0.1` (Default Gateway), `1.1.1.1` (Cloudflare), `8.8.8.8` (Google), and `9.9.9.9` (Quad9) on first boot.
- **Dynamic Target Management:** Add, pause, resume, reset, or delete targets live from the WebUI without restarting the container.
- **PingPlotter-Style Dual-Pane WebUI:**
  - **Sidebar:** Live status badges, current latency, and 1-hour packet loss % across all targets.
  - **Upper Pane (Hop Table):** Full hop-by-hop table with inline min/avg/max latency bars, 30-sample SVG sparklines, and checkboxes to toggle plotting any intermediate hop on the timeline.
  - **Lower Pane (Live Timeline Graph):** Real-time scrolling latency line chart and vertical red packet-loss bars with `5m`, `15m`, `1h`, `6h`, and `24h` time windows plus zoom/pan controls.
- **Automatic Database Pruning:** Stores time-series data in `/data/networkstats.db` (SQLite WAL mode) and automatically prunes metrics older than 48 hours.

---

## Deploying with Portainer (Git Repository Method)

Portainer can pull and build this project directly from your GitHub repository:

1. Open **Portainer** and go to **Stacks** → **+ Add stack**.
2. Set **Name** to `networkstats`.
3. Select **Repository** as the build method.
4. Fill in the repository settings:
   - **Repository URL:** `https://github.com/<YOUR_GITHUB_USERNAME>/NetworkStats`
   - **Repository reference:** `refs/heads/main`
   - **Compose path:** `docker-compose.yml`
5. Click **Deploy the stack**.
6. Open `http://<YOUR_SERVER_IP>:8085` in your browser.

---

## Deploying with Portainer (Web Editor + Prebuilt GHCR Image)

This repository includes a GitHub Actions workflow (`.github/workflows/docker-publish.yml`) that automatically builds and publishes a multi-arch (`amd64`/`arm64`) Docker image to GitHub Container Registry (`ghcr.io/<your-username>/networkstats:latest`) on every push to `main`.

To deploy via Portainer's **Web editor** using the prebuilt image:

```yaml
version: "3.8"

services:
  networkstats:
    image: ghcr.io/<your-username>/networkstats:latest
    container_name: networkstats
    restart: unless-stopped
    ports:
      - "8085:8000"
    volumes:
      - /opt/networkstats/data:/data
    environment:
      - TZ=America/New_York
      - DB_PATH=/data/networkstats.db
      - PROBE_INTERVAL=2.0
      - RETENTION_HOURS=48
    cap_add:
      - NET_RAW
      - NET_ADMIN
```

---

## Deploying via Docker Compose CLI

```bash
git clone https://github.com/<YOUR_GITHUB_USERNAME>/NetworkStats.git
cd NetworkStats
docker compose up -d --build
```

---

## Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `TZ` | `America/New_York` | Container timezone |
| `DB_PATH` | `/data/networkstats.db` | SQLite database file path |
| `PROBE_INTERVAL` | `2.0` | Default MTR probe interval in seconds |
| `RETENTION_HOURS` | `48` | Rolling retention window before automatic pruning |
