# Docker Deployment & Portainer Guide

This guide provides the necessary configuration to deploy the **PCO to QuickBooks Sync** application using Docker and Portainer.

## Docker Compose Configuration

The following `docker-compose.yml` is optimized for production. It uses port **8337** and mounts only the necessary persistent data volumes.

```yaml
version: "3.8"
  pco-qb-sync:
    image: pco-qb-sync:latest # Ensure you build the image first or point to your registry
    container_name: pco-qb-sync
    restart: unless-stopped
    ports:
      - "8337:8080"
    volumes:
      - ./config:/app/config
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - TZ=America/Chicago
      # Portainer: Add the environment variables listed below
```

## Persistent Volume Mounts

To ensure your configuration and sync history persist between container updates, you MUST bind the following host paths to the container:

| Host Path | Container Path | Description |
|-----------|----------------|-------------|
| `./config` | `/app/config` | Stores `.env` and `config.json` |
| `./data` | `/app/data` | Stores all persistent state (auth, sync history, cursor) |
| `./logs` | `/app/logs` | Sync logs and server logs |

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

## Post-Deployment
1. The dashboard will be accessible at `http://<your-server-ip>:8337`.
2. Ensure you update your **QuickBooks App Redirect URI** and **Entra ID Redirect URI** to match the new port/URL if necessary.
