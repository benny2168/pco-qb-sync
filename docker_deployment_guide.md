## Deployment via Portainer (GitHub Repository)

For Synology deployments, it is recommended to use the **Repository** method in Portainer to avoid build context errors (e.g., "failed to read dockerfile").

### 1. Create a New Stack
1. In Portainer, go to **Stacks** > **Add stack**.
2. Select **Repository** as the build method.
3. **Repository URL**: `https://github.com/benny2168/pco-qb-sync.git`
4. **Repository reference**: `refs/heads/update-portainer` (or `main` once merged).
5. **Compose path**: `docker-compose.yml`

### 2. Docker Compose Configuration
The `docker-compose.yml` is automatically pulled from your GitHub repository. Since the repository already contains the updated volume mappings, you don't need to manually enter them in Portainer.

> [!IMPORTANT]
> **Host Preparation**: Before deploying the stack, you must manually create the following folders on your Synology NAS (e.g., via File Station):
> *   `/volume1/docker/pco-qb-sync/config` (Place your `.env` and `config.json` here)
> *   `/volume1/docker/pco-qb-sync/data`
> *   `/volume1/docker/pco-qb-sync/logs`

When using the **Repository** method, the `build: .` directive works correctly because Portainer clones the entire repository onto the NAS.

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

## Environment Variables Management

You have two options for managing your environment variables:

### Option A: Folder-Based (Recommended)
Simply place your `.env` file into the `/volume1/docker/pco-qb-sync/config` folder on your Synology. 
*   **Pros**: You don't need to touch the Portainer UI for environment variables.
*   **How it works**: The application is configured to look for `.env` inside `/app/config/` automatically.

### Option B: Portainer UI
You can also enter the variables directly in the Portainer **Environment variables** section when creating the stack.
*   **Pros**: Easy to edit directly in the browser.
*   **Note**: If you do this, you don't need a physical `.env` file on the NAS.

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

### "Bind mount failed: ... does not exist"
This means Docker cannot find the folder on your Synology NAS. 
1.  **Verify the Absolute Path**: 
    *   Open **File Station**.
    *   Right-click the `pco-qb-sync` folder.
    *   Select **Properties**.
    *   Copy the **Location** field (e.g., `/volume1/docker/pco-qb-sync`). 
    *   If yours says `/volume2/...` or something different, you **must** update the `docker-compose.yml` to match.
2.  **Manual Creation**: Ensure you have actually created the `config`, `data`, and `logs` subfolders inside `pco-qb-sync`.
3.  **Permissions**: Ensure the `docker` user group has Read/Write permissions to these folders.

### "failed to read dockerfile" or "config.json not found"
1.  **Use Repository Method**: Always use the **Repository** deployment method described at the top of this guide.
2.  **Clean Build**: I have updated the `Dockerfile` to no longer require `config.json` at build time. It is now loaded from your volume mount at runtime. Pull the latest from the `main` branch.

### Accessing the Dashboard
1. The dashboard will be accessible at `http://<your-server-ip>:8337`.
2. Ensure you update your **QuickBooks App Redirect URI** and **Entra ID Redirect URI** to match the new port/URL if necessary.
