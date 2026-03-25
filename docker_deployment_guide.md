## Deployment via Portainer (GitHub Repository)

For Synology deployments, it is recommended to use the **Repository** method in Portainer to avoid build context errors (e.g., "failed to read dockerfile").

### 1. Create a New Stack
1. In Portainer, go to **Stacks** > **Add stack**.
2. Select **Repository** as the build method.
3. **Repository URL**: `https://github.com/benny2168/pco-qb-sync.git`
4. **Repository reference**: `refs/heads/update-portainer` (or `main` once merged).
5. **Compose path**: `docker-compose.yml`

### 2. Docker Compose Configuration
Ensure your `docker-compose.yml` uses the following structure. 

> [!IMPORTANT]
> When using the **Repository** method, the `build: .` directive works correctly because Portainer clones the entire repository onto the NAS.

```yaml
version: "3.8"

services:
  pco-qb-sync:
    build: .
    container_name: pco-qb-sync
    restart: unless-stopped
    ports:
      - "8337:8080"
    volumes:
      - /volume1/docker/pco-qb-sync/config:/app/config
      - /volume1/docker/pco-qb-sync/data:/app/data
      - /volume1/docker/pco-qb-sync/logs:/app/logs
    environment:
      - TZ=America/Chicago
      # Portainer: Add the environment variables listed below
```

## Persistent Volume Mounts

To ensure your configuration and sync history persist between container updates, you MUST bind the following host paths to the container. 

> [!TIP]
> **Synology Absolute Paths**: On Synology, absolute paths usually start with `/volume1/`. You can find the exact path by right-clicking a folder in **File Station** > **Properties** > **Location**.

| Host Path (Synology Example) | Container Path | Description |
|------------------------------|----------------|-------------|
| `/volume1/docker/pco-qb-sync/config` | `/app/config` | Stores `.env` and `config.json` |
| `/volume1/docker/pco-qb-sync/data` | `/app/data` | Stores all persistent state (auth, sync history, cursor) |
| `/volume1/docker/pco-qb-sync/logs` | `/app/logs` | Sync logs and server logs |

> [!TIP]
> **Migration Note**: If you are upgrading from an older version, move your `.env` and `config.json` files from the root into the `./config/` directory on your host before starting the container.

> [!IMPORTANT]
> Ensure the user running the Docker daemon has write permissions to these host directories/files.

## Portainer Environment Variables

When deploying via Portainer, enter the following keys in the **Environment variables** section:

| Variable | Description |
|----------|-------------|
| `PCO_PAT` | Planning Center Personal Access Token |
| `PCO_APP_ID` | Planning Center Application ID |
| `QB_CLIENT_ID` | QuickBooks Developer Client ID |
| `QB_CLIENT_SECRET` | QuickBooks Developer Client Secret |
| `QB_REFRESH_TOKEN` | Initial QuickBooks Refresh Token |
| `QB_REALM_ID` | QuickBooks Company ID (Realm ID) |
| `SMTP_SERVER` | SMTP Server for email notifications |
| `SMTP_PORT` | SMTP Port (usually 587) |
| `SMTP_SENDER_EMAIL`| From address for sync reports |
| `SMTP_PASSWORD` | SMTP Authentication Password |
| `SMTP_RECIPIENT_EMAIL`| Admin email to receive reports |
| `SYNC_SCHEDULE` | Cron expression for sync (e.g., `0 0 3 * * 1`) |
| `AZURE_CLIENT_ID` | Entra ID (Azure) Application ID |
| `AZURE_CLIENT_SECRET`| Entra ID Application Secret |
| `AZURE_TENANT_ID` | Entra ID Tenant ID |
| `AZURE_GROUP_ID` | Entra ID Group ID for restricted access |
| `AZURE_REDIRECT_PATH`| Callback path (usually `/callback`) |
| `AZURE_SCOPE` | Entra ID Scopes (`User.Read GroupMember.Read.All`) |
| `FLASK_SECRET_KEY` | Random string for session security |

## Troubleshooting

### "failed to read dockerfile: no such file or directory"
This error occurs in Portainer when using the **Web Editor** (copy-pasting the compose file) while using the `build: .` directive.
*   **Why?**: The Web Editor does not have access to the `Dockerfile` or source code on your NAS.
*   **Solution**: Use the **Repository** deployment method described above. This allows Portainer to pull the `Dockerfile` and source code directly from GitHub into a temporary build context on the NAS.

### Accessing the Dashboard
1. The dashboard will be accessible at `http://<your-server-ip>:8337`.
2. Ensure you update your **QuickBooks App Redirect URI** and **Entra ID Redirect URI** to match the new port/URL if necessary.
