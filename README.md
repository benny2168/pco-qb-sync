# PCO to QuickBooks Sync

A high-performance Docker-based tool that synchronizes membership data from **Planning Center Online** (PCO) to **QuickBooks Online** (QBO) as customers. Specially designed for church administration.

## Features
- 🔄 **Bidirectional Sync Insights**: Live dashboard showing real-time sync progress and duration.
- 🔐 **Dual Auth Systems**: 
  - **Microsoft Azure SSO**: Secure, group-based access control.
  - **Local Admin Failsafe**: Password-protected secondary access if SSO is unavailable.
- 📋 **Advanced Logging**: Interactive accordion-style log viewer with keyword search and auto-scrolling.
- 📧 **Automated Reporting**: Configurable email summaries sent after every sync.
- 📂 **Persistent History**: Tracks every record created or updated to prevent duplicates.
- ⚡ **Resilient Processing**: Built-in rate-limit handling and atomic JSON persistence for data integrity.
- 🐳 **Docker Ready**: Easy deployment on NAS or cloud servers.

## Quick Start

### 1. Clone & Configure
```bash
git clone https://github.com/YOUR_USERNAME/pco-qb-sync.git
cd pco-qb-sync
cp .env.example .env
# Edit .env and config.json with your credentials
```

### 2. Run with Docker Compose
```bash
docker compose up -d
```

### 3. Access Dashboard
Open `http://your-server-ip:8337`

## Configuration

### Microsoft Azure SSO Setup
1. Register a new Web App in Entra ID (Azure AD).
2. Set the Redirect URI to `https://your-domain.com/callback`.
3. Create a Client Secret.
4. Add the following to `.env`:
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`
   - `AZURE_TENANT_ID`
   - `AZURE_GROUP_ID` (Security group for authorized users)

### Local Admin Setup
The system includes a local failsafe login.
- **Default User**: `admin`
- **Default Pass**: `admin1234`
- SSO users can change the password or disable local login from the dashboard settings.

## Environment Variables (`.env`)
| Variable | Description |
|---|---|
| `FLASK_SECRET_KEY` | Secret for session signing |
| `PCO_PAT` | Planning Center Personal Access Token |
| `QB_CLIENT_ID` | QuickBooks OAuth Client ID |
| `QB_CLIENT_SECRET` | QuickBooks OAuth Client Secret |
| `AZURE_REDIRECT_URI_OVERRIDE` | Mandatory if running behind a proxy (e.g., https://sync.church.org) |

## Data Persistence
The container uses a volume mount for `./data:/app/data`. This store contains:
- `auth_settings.json`: Local admin credentials.
- `sync_history.json`: Audit trail of synced records.
- `donation_sync_state.json`: Trackers for donation synchronization.

## License
MIT
