# PCO to QuickBooks Sync

A Docker-based sync tool that automatically synchronizes membership data from **Planning Center Online** to **QuickBooks** as customers.

## Features
- 🔄 Scheduled cron-based sync (configurable via `SYNC_SCHEDULE`)
- 🖥️ Live dashboard with real-time log streaming
- 📋 Accordion-style log viewer with keyword search
- 📧 Configurable email summary reports
- 🔐 OAuth 2.0 token auto-rotation for QuickBooks
- ⚡ Rate-limit handling with exponential backoff
- 🐳 Docker-ready for NAS / server deployment

## Quick Start

### 1. Clone & Configure
```bash
git clone https://github.com/YOUR_USERNAME/pco-qb-sync.git
cd pco-qb-sync
cp .env.example .env
# Edit .env with your credentials
```

### 2. Run with Docker Compose
```bash
docker compose up -d
```

### 3. Access Dashboard
Open `http://your-server-ip:8377/dashboard`

## Environment Variables (`.env`)
| Variable | Description |
|---|---|
| `PCO_APP_ID` | Planning Center Application ID |
| `PCO_PAT` | Planning Center Personal Access Token |
| `QB_CLIENT_ID` | QuickBooks OAuth Client ID |
| `QB_CLIENT_SECRET` | QuickBooks OAuth Client Secret |
| `QB_REFRESH_TOKEN` | QuickBooks OAuth Refresh Token (auto-rotated) |
| `QB_REALM_ID` | QuickBooks Company/Realm ID |
| `SYNC_SCHEDULE` | Cron schedule (6-field Azure format, e.g. `0 0 3 * * 1`) |
| `SMTP_SERVER` | SMTP server for email reports |
| `SMTP_PORT` | SMTP port (default: 587) |
| `SMTP_SENDER_EMAIL` | Sender email address |
| `SMTP_PASSWORD` | SMTP password |
| `SMTP_RECIPIENT_EMAIL` | Recipient for sync reports |

## Portainer Stack
Use the `docker-compose.yml` in this repo directly as a Portainer stack.

## License
MIT
