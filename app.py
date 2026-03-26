"""
PCO to QuickBooks Sync — Standalone Flask App
Self-hosted web server + scheduler via Docker.
"""
import os
import json
import glob
import logging
import threading
import time
import errno
from datetime import datetime
from functools import wraps

from flask import Flask, request, jsonify, send_file, session, redirect, url_for, render_template
from flask_session import Session
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
import msal
import requests

from sync_pc_to_qb import SyncRoutine, load_config, setup_logging, rotate_logs
import sync_pc_to_qb
print(f"DEBUG: sync_pc_to_qb file path: {sync_pc_to_qb.__file__}")
from sync_donations_qb_to_pc import PlanningCenterGivingClient

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
# Priority: /app/config/.env, Fallback: /app/.env
ENV_PATH = os.path.join(BASE_DIR, 'config', '.env')
if not os.path.isfile(ENV_PATH):
    fallback_path = os.path.join(BASE_DIR, '.env')
    if os.path.isfile(fallback_path):
        ENV_PATH = fallback_path

# Prefer .env file over Docker environment variables for rotating tokens
load_dotenv(dotenv_path=ENV_PATH, override=True)
logging.info(f"Loaded environment from: {ENV_PATH}")
# Log available keys (masked)
keys = [k for k in os.environ.keys() if k.startswith(('PCO_', 'QB_', 'MAILCHIMP_', 'CHURCH_'))]
logging.info(f"Available App Env Keys: {keys}")

AUTH_SETTINGS_PATH = os.path.join(BASE_DIR, 'data', 'auth_settings.json')

def get_auth_settings():
    """Retrieve auth settings, creating defaults if missing."""
    if os.path.exists(AUTH_SETTINGS_PATH):
        try:
            return read_json_with_retries(AUTH_SETTINGS_PATH)
        except Exception:
            pass
    
    # Default: admin / admin1234
    default_settings = {
        "local_admin_user": "admin",
        "local_admin_password_hash": generate_password_hash("admin1234"),
        "local_login_enabled": True
    }
    os.makedirs(os.path.dirname(AUTH_SETTINGS_PATH), exist_ok=True)
    save_json_with_retries(AUTH_SETTINGS_PATH, default_settings)
    return default_settings

def save_auth_settings(settings):
    """Save auth settings to file."""
    return save_json_with_retries(AUTH_SETTINGS_PATH, settings)

def read_json_with_retries(path, retries=5, delay=0.1):
    """Attempt to read a JSON file with retries to handle transient file lock/deadlock errors."""
    for i in range(retries):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except OSError as e:
            if e.errno == 35: # Resource deadlock avoided (Errno 35 on BSD/macOS)
                if i < retries - 1:
                    time.sleep(delay)
                    continue
            raise
        except json.JSONDecodeError:
            # Handle possible partial write (though atomic rename should prevent this)
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise
    return None

def robust_save_file(path, content, is_json=True, retries=5, delay=0.1):
    """Attempt to save a file atomically with retries and a direct-write fallback."""
    for i in range(retries):
        temp_path = f"{path}.tmp"
        try:
            with open(temp_path, 'w', encoding='utf-8') as f:
                if is_json:
                    json.dump(content, f, indent=4)
                else:
                    f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, path)
            return True
        except OSError as e:
            if e.errno == 16: # Device or resource busy
                logging.warning(f"Atomic rename failed (errno 16) for {path}. Falling back to direct write.")
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        if is_json:
                            json.dump(content, f, indent=4)
                        else:
                            f.write(content)
                    return True
                except Exception as ex:
                    logging.error(f"Fallback write also failed for {path}: {ex}")
                    return False
            if i < retries - 1:
                time.sleep(delay)
                continue
            logging.error(f"Failed to save {path} after {retries} retries: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error saving {path}: {e}")
            return False
    return False

def save_json_with_retries(path, data, retries=5, delay=0.1):
    return robust_save_file(path, data, is_json=True, retries=retries, delay=delay)

def update_env_file_bulk(updates):
    """Update or add multiple key-value pairs in the .env file while preserving comments."""
    if not os.path.exists(ENV_PATH):
        # Create empty .env if missing
        with open(ENV_PATH, 'w') as f:
            pass
            
    with open(ENV_PATH, 'r') as f:
        lines = f.readlines()
    
    # Track which keys we've already found in the file
    processed_keys = set()
    new_lines = []
    
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            new_lines.append(line)
            continue
            
        parts = stripped.split('=', 1)
        if len(parts) == 2:
            key = parts[0].strip()
            if key in updates:
                new_lines.append(f"{key}='{updates[key]}'\n")
                processed_keys.add(key)
                continue
        new_lines.append(line)
        
    # Add any new keys that weren't in the file
    for key, value in updates.items():
        if key not in processed_keys:
            new_lines.append(f"{key}='{value}'\n")
            
    if robust_save_file(ENV_PATH, "".join(new_lines), is_json=False):
        # Reload immediately
        load_dotenv(dotenv_path=ENV_PATH, override=True)
        return True
def update_env_file(key, value):
    """Compatibility wrapper for single key updates."""
    return update_env_file_bulk({key: value})

def mask_value(val):
    if not val: return ""
    if len(val) <= 12: return "********"
    return f"{val[:6]}...{val[-6:]}"

SENSITIVE_KEYS = [
    'PCO_PAT', 'PCO_APP_ID', 'QB_CLIENT_ID', 'QB_CLIENT_SECRET', 'QB_REFRESH_TOKEN',
    'SMTP_PASSWORD', 'AZURE_CLIENT_SECRET', 'FLASK_SECRET_KEY'
]

# Ensure a secure secret key exists
if not os.getenv('FLASK_SECRET_KEY'):
    import secrets
    new_key = secrets.token_hex(32)
    logging.info("FLASK_SECRET_KEY missing. Generating a new secure key...")
    update_env_file_bulk({'FLASK_SECRET_KEY': new_key})
    os.environ['FLASK_SECRET_KEY'] = new_key

CONFIG_HINTS = {
    'PCO_PAT': 'Personal Access Token from PCO Developer Settings',
    'PCO_APP_ID': 'Application ID for your custom PCO integration',
    'PCO_LIST_ID': 'The ID of the PCO List to sync (e.g. 4661587)',
    'QB_CLIENT_ID': 'QuickBooks Developer Portal Client ID',
    'QB_CLIENT_SECRET': 'QuickBooks Developer Portal Client Secret',
    'QB_REFRESH_TOKEN': 'Rotating OAuth token (updated automatically)',
    'QB_REALM_ID': 'Company ID found in QuickBooks Account Settings',
    'SMTP_SERVER': 'Hostname of your email provider (e.g. smtp.gmail.com)',
    'SMTP_PORT': 'Port number (usually 587 for STARTTLS)',
    'SMTP_SENDER_EMAIL': 'The email address used to send sync reports',
    'SMTP_PASSWORD': 'SMTP Authentication Password',
    'SMTP_RECIPIENT_EMAIL': 'Admin email to receive reports',
    'SYNC_SCHEDULE': 'Cron expression for sync (e.g. 0 0 3 * * 1)',
    'AZURE_CLIENT_ID': 'Application (client) ID from Azure Portal',
    'AZURE_TENANT_ID': 'Directory (tenant) ID from Azure Portal',
    'AZURE_CLIENT_SECRET': 'Client Secret from Azure Portal',
    'AZURE_GROUP_ID': 'Entra ID Group ID for restricted access',
    'AZURE_REDIRECT_PATH': 'Callback path (usually /callback)',
    'AZURE_SCOPE': 'Entra ID Scopes (space separated)',
    'AZURE_REDIRECT_URI_OVERRIDE': 'Optional production URL override',
    'FLASK_SECRET_KEY': 'Random string for session security',
    'LOG_LEVEL': 'Logging detail (INFO, DEBUG, WARNING, ERROR)',
    'FLASK_PORT': 'Internal port for the web server (default 8080)',
    'PCO_CONFIG_DIR': 'Absolute path to config folder on host',
    'PCO_DATA_DIR': 'Absolute path to data folder (auth, history)',
    'PCO_LOGS_DIR': 'Absolute path to logs folder'
}


app = Flask(__name__, static_folder='static', static_url_path='/static')
# Handle reverse proxy headers (X-Forwarded-Proto, X-Forwarded-Host)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

@app.after_request
def add_security_headers(response):
    """Add standard security headers to all responses."""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    # Recommended: Content-Security-Policy (CSP) - start relaxed if needed
    # response.headers['Content-Security-Policy'] = "default-src 'self';"
    return response

def verify_origin(f):
    """Basic CSRF mitigation: verify request origin matches host."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == 'POST':
            origin = request.headers.get('Origin')
            host = request.headers.get('Host')
            # In some setups behind proxies, Host might not match Origin exactly (proto vs no proto)
            # but we can check if host is part of origin
            if origin and host not in origin:
                 logging.warning(f"CSRF Alert: Origin '{origin}' does not match Host '{host}'")
                 return jsonify({"error": "Forbidden: CSRF protection triggered."}), 403
        return f(*args, **kwargs)
    return decorated_function

# Ensure secret key is consistent
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'change-this-to-something-very-secret')
app.config['SESSION_COOKIE_NAME'] = 'pco_qb_sync_session'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
# Standard Flask signed cookies (no special SESSION_TYPE needed)

# OAuth Config
CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
TENANT_ID = os.getenv('AZURE_TENANT_ID')
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID or 'common'}"
REDIRECT_PATH = os.getenv('AZURE_REDIRECT_PATH', '/callback')
# Scope is stored as space-separated string in .env
SCOPE_STR = os.getenv('AZURE_SCOPE', 'User.Read GroupMember.Read.All')
SCOPE = SCOPE_STR.split() 
GROUP_ID = os.getenv('AZURE_GROUP_ID')
# Optional override for production (e.g. https://sync.church.org/callback)
REDIRECT_URI_OVERRIDE = os.getenv('AZURE_REDIRECT_URI_OVERRIDE')

def get_msal_app():
    return msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user'):
            if request.path.startswith('/api/'):
                return jsonify({"error": "Unauthorized"}), 401
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ---------------------------------------------------------------------------
# Scheduler Setup
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(daemon=True)

def parse_cron(expr: str):
    """Convert a 6-field CRON expression (sec min hour day month dow) to APScheduler fields."""
    parts = expr.strip().split()
    if len(parts) != 6:
        return None
    sec, minute, hour, day, month, dow = parts
    return {
        'second': sec,
        'minute': minute,
        'hour': hour,
        'day': day,
        'month': month,
        'day_of_week': dow,
    }

def run_scheduled_sync():
    """Callback executed by APScheduler on the cron schedule."""
    logging.info("Scheduled sync triggered by APScheduler cron job.")
    try:
        # Use standardized config path
        config_path = os.path.join(BASE_DIR, 'config', 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(BASE_DIR, 'config.json')
        config = load_config(config_path)
        setup_logging(config)
        rotate_logs(keep=10)
        routine = SyncRoutine(config)
        routine.run()
    except Exception as e:
        logging.error(f"Scheduled sync failed: {e}", exc_info=True)

def start_scheduler():
    """Start the APScheduler with the SYNC_SCHEDULE from env."""
    if not scheduler.running:
        scheduler.start()
    reschedule_sync()
    reschedule_donation_sync()

def reschedule_sync():
    """Add or update the sync cron job based on current SYNC_SCHEDULE env var."""
    schedule_expr = os.getenv('SYNC_SCHEDULE', '')
    if not schedule_expr:
        logging.warning("SYNC_SCHEDULE not set. No automatic sync will run.")
        # Remove existing job if schedule is cleared
        try:
            scheduler.remove_job('pco_qb_sync')
        except Exception:
            pass
        return

    cron_fields = parse_cron(schedule_expr)
    if not cron_fields:
        logging.error(f"Invalid SYNC_SCHEDULE expression: {schedule_expr}")
        return

    trigger = CronTrigger(**cron_fields)
    scheduler.add_job(run_scheduled_sync, trigger, id='pco_qb_sync', replace_existing=True)
    logging.info(f"Scheduler (re)configured with SYNC_SCHEDULE = {schedule_expr}")

def run_scheduled_donation_sync():
    """Callback for scheduled donation sync."""
    logging.info("Scheduled donation sync triggered.")
    try:
        # Load main config (priority: /app/config/config.json)
        config_path = os.path.join(BASE_DIR, 'config', 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(BASE_DIR, 'config.json')
        config = load_config(config_path)
        
        # Load donation settings (priority: /app/data/donation_sync_settings.json)
        settings_path = os.path.join(BASE_DIR, 'data', 'donation_sync_settings.json')
        if not os.path.exists(settings_path):
             settings_path = os.path.join(BASE_DIR, 'donation_sync_settings.json')
             
        donation_settings = {}
        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                donation_settings = json.load(f)
        
        log_file = setup_logging(config, prefix="donations_sync")
        rotate_logs(keep=10)
        
        # Save initial status in data/
        status_path = os.path.join(BASE_DIR, 'data', 'latest_donation_sync_status.json')
        with open(status_path, 'w') as f:
            json.dump({"status": "Running", "log_file": os.path.basename(log_file)}, f)

        from sync_donations_qb_to_pc import DonationSyncRoutine
        routine = DonationSyncRoutine(config, donation_settings)
        routine.run()
    except Exception as e:
        logging.error(f"Scheduled donation sync failed: {e}", exc_info=True)

def reschedule_donation_sync():
    """Update the donation sync job based on donation_sync_settings.json (priority: data/)."""
    settings_path = os.path.join(BASE_DIR, 'data', 'donation_sync_settings.json')
    if not os.path.exists(settings_path):
        settings_path = os.path.join(BASE_DIR, 'donation_sync_settings.json')
        
    if not os.path.exists(settings_path):
        return

    try:
        with open(settings_path, 'r') as f:
            settings = json.load(f)
        
        freq = settings.get('sync_frequency', 'manual')
        if freq == 'manual':
            try:
                scheduler.remove_job('donation_sync')
            except Exception:
                pass
            return

        # Handle 6-field cron expression
        if len(freq.split(' ')) == 6:
            cron_fields = parse_cron(freq)
            if cron_fields:
                trigger = CronTrigger(**cron_fields)
                scheduler.add_job(run_scheduled_donation_sync, trigger, id='donation_sync', replace_existing=True)
                logging.info(f"Donation scheduler (re)configured with cron = {freq}")
                return

        # Fallback/Legacy simple mapping
        if freq == 'daily':
            trigger = CronTrigger(hour=1, minute=0)
        elif freq == 'hourly':
            trigger = CronTrigger(minute=0)
        else:
            logging.warning(f"Unknown frequency '{freq}', skipping donation schedule.")
            return

        scheduler.add_job(run_scheduled_donation_sync, trigger, id='donation_sync', replace_existing=True)
        logging.info(f"Donation scheduler (re)configured with frequency = {freq}")
    except Exception as e:
        logging.error(f"Failed to reschedule donation sync: {e}")

# ---------------------------------------------------------------------------
# Routes — Auth
# ---------------------------------------------------------------------------
@app.route("/login")
def login():
    """Initiates the Microsoft OAuth login flow."""
    try:
        logging.info("Initiating Microsoft login flow.")
        msal_app = get_msal_app()
        
        # Robust redirect_uri construction
        if REDIRECT_URI_OVERRIDE:
            redirect_uri = REDIRECT_URI_OVERRIDE
            # Ensure the path is included if the override was just a domain
            if REDIRECT_PATH not in redirect_uri:
                redirect_uri = redirect_uri.rstrip('/') + REDIRECT_PATH
        else:
            redirect_uri = url_for("authorized", _external=True)
            
        logging.info(f"Using redirect_uri: {redirect_uri}")
        
        flow = msal_app.initiate_auth_code_flow(
            SCOPE, redirect_uri=redirect_uri
        )
        session["flow"] = flow
        logging.debug(f"Stored flow in session: {bool(session.get('flow'))}")
        return redirect(flow["auth_uri"])
    except Exception as e:
        logging.exception("Failed to initiate login flow")
        return f"Internal error during login initiation: {e}", 500

@app.route(REDIRECT_PATH)
def authorized():
    """Callback for Microsoft OAuth."""
    try:
        logging.info("Callback received from Microsoft.")
        
        flow = session.pop("flow", None)
        if not flow:
            logging.error("No flow found in session. Session may have expired or been lost.")
            return "Login failure: No active session flow found. Please try logging in again.", 401
            
        msal_app = get_msal_app()
        result = msal_app.acquire_token_by_auth_code_flow(flow, request.args)
        
        if "error" in result:
            logging.error(f"MSAL acquire_token error: {result.get('error')} - {result.get('error_description')}")
            return f"Login failure: {result.get('error_description')}", 401
        
        access_token = result.get("access_token")
        if not access_token:
            logging.error("No access token returned from Microsoft.")
            return "Login failure: No access token received.", 401
            
        # Check transitive membership to the target group
        logging.info(f"Checking group membership for GROUP_ID: {GROUP_ID}")
        graph_url = f"https://graph.microsoft.com/v1.0/me/memberOf/microsoft.graph.group?$filter=id eq '{GROUP_ID}'"
        headers = {'Authorization': 'Bearer ' + access_token}
        resp = requests.get(graph_url, headers=headers)
        
        if resp.status_code != 200:
            logging.error(f"Failed to check group membership: {resp.status_code} - {resp.text}")
            return f"Login failure: Could not verify group membership ({resp.status_code})", 401
            
        groups = resp.json().get('value', [])
        logging.debug(f"User is member of {len(groups)} groups matching criteria.")
        is_member = any(g.get('id') == GROUP_ID for g in groups)
        
        if not is_member:
            logging.warning("User is not a member of the required access group.")
            return "Access Denied: You are not a member of the required access group.", 403

        user_claims = result.get("id_token_claims")
        user_claims["is_sso"] = True
        session["user"] = user_claims
        logging.info(f"User {session['user'].get('preferred_username')} logged in successfully.")
        return redirect(url_for("index"))
        
    except Exception as e:
        logging.exception("Authorized callback error")
        return f"Internal authentication error: {e}", 500

@app.route("/logout")
def logout():
    # Clear the local Flask session
    session.clear()
    
    post_logout_uri = REDIRECT_URI_OVERRIDE or url_for('index', _external=True)
    if REDIRECT_URI_OVERRIDE:
        # If we have an override, we need the base part (strip /callback)
        post_logout_uri = REDIRECT_URI_OVERRIDE.replace(REDIRECT_PATH, '')
    
    return redirect(
        f"{AUTHORITY}/oauth2/v2.0/logout?post_logout_redirect_uri={post_logout_uri}"
    )

# ---------------------------------------------------------------------------
# Routes — Dashboard
# ---------------------------------------------------------------------------
@app.route('/dashboard')
@login_required
def dashboard_page():
    return render_template('dashboard.html', user=session.get('user'))

@app.route('/')
def index():
    if not session.get('user'):
        auth_settings = get_auth_settings() or {}
        local_enabled = auth_settings.get('local_login_enabled', True)
        return render_template('login.html', local_enabled=local_enabled)
    return dashboard_page()

@app.route('/local-login', methods=['GET', 'POST'])
def local_login():
    if session.get('user'):
        return redirect(url_for('dashboard_page'))
    
    error = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        auth_settings = get_auth_settings()
        if not auth_settings.get('local_login_enabled', True):
            error = "Local login is currently disabled."
        elif username == auth_settings.get('local_admin_user') and \
             check_password_hash(auth_settings.get('local_admin_password_hash'), password):
            session['user'] = {
                'name': 'Local Admin',
                'preferred_username': username,
                'is_sso': False
            }
            return redirect(url_for('dashboard_page'))
        else:
            error = "Invalid username or password."
            
    return render_template('local_login.html', error=error)

@app.route('/api/auth/local-settings', methods=['GET', 'POST'])
@login_required
@verify_origin
def api_auth_local_settings():
    """Manage local login settings. Restricted to SSO users."""
    auth_settings = get_auth_settings() or {}
    user = session.get('user', {})
    is_sso = user.get('is_sso', False) or user.get('oid') is not None
    is_local_admin = not is_sso and user.get('preferred_username') == auth_settings.get('local_admin_user')
    
    if not (is_sso or is_local_admin):
        logging.warning(f"Unauthorized access attempt to local settings from user: {user.get('preferred_username')}")
        return jsonify({"error": "Admin privileges required to manage local login settings."}), 403
    
    if request.method == 'POST':
        data = request.json
        logging.info(f"Received local settings update request: {data}")
        
        if 'enabled' in data:
            auth_settings['local_login_enabled'] = bool(data['enabled'])
            logging.info(f"Local login enabled state set to: {auth_settings['local_login_enabled']}")
        
        # Support both 'password' and 'new_password' for robustness, though frontend uses 'new_password'
        new_pass = data.get('new_password') or data.get('password')
        if new_pass:
            auth_settings['local_admin_password_hash'] = generate_password_hash(new_pass)
            logging.info("Local admin password hash regenerated.")
            
        if save_auth_settings(auth_settings):
            logging.info("Auth settings saved successfully to disk.")
            return jsonify({"success": True, "enabled": auth_settings['local_login_enabled']})
        else:
            logging.error("Failed to save auth settings to disk.")
            return jsonify({"error": "Failed to save settings file."}), 500
    
    return jsonify({
        "enabled": auth_settings.get('local_login_enabled', True),
        "username": auth_settings.get('local_admin_user')
    })
    return dashboard_page()

@app.route('/api/me')
@login_required
def api_me():
    """Returns the logged-in user's profile info."""
    user = session.get('user', {})
    return jsonify({
        "name": user.get("name"),
        "email": user.get("preferred_username") or user.get("email"),
        "oid": user.get("oid")
    })

# ---------------------------------------------------------------------------
# Routes — API (Protected)
# ---------------------------------------------------------------------------
@app.route('/api/status')
@login_required
def api_status():
    # Reload settings periodically; prefer .env for rotating tokens
    load_dotenv(dotenv_path=ENV_PATH, override=True)

    status_path = os.path.join(BASE_DIR, 'data', 'latest_sync_status.json')
    if not os.path.exists(status_path):
        status_data = {"status": "Idle", "last_summary": {"created": 0, "updated": 0, "errors": 0}}
    else:
        try:
            status_data = read_json_with_retries(status_path)
        except Exception as e:
            logging.error(f"Error reading status json: {e}")
            status_data = {"status": "Idle", "error": "Failed to read status file"}

    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_files = sorted(glob.glob(os.path.join(log_dir, "sync_*.log")), key=os.path.getmtime, reverse=True)
    # Return just filenames (not full paths), excluding donation logs
    log_basenames = [os.path.basename(f) for f in log_files if not os.path.basename(f).startswith("donations_sync_")]

    # Get next run time for Member Sync
    next_run = None
    job = scheduler.get_job('pco_qb_sync')
    if job and job.next_run_time:
        next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")

    # Check QB connectivity briefly
    qb_connected = False
    if os.getenv("QB_REFRESH_TOKEN") and os.getenv("QB_CLIENT_ID"):
        qb_connected = True

    return jsonify({
        "status": status_data,
        "logs": log_basenames,
        "schedule": os.getenv("SYNC_SCHEDULE", "Not Set"),
        "next_run": next_run,
        "recipient_email": os.getenv("SMTP_RECIPIENT_EMAIL", ""),
        "pco_list_id": os.getenv("PCO_LIST_ID", "2552744"),
        "qb_connected": qb_connected,
        "display_name_format": load_config().get('planning_center', {}).get('display_name_format', '{first_name} {last_name}')
    })

@app.route('/api/logs/<filename>')
@login_required
def api_logs(filename):
    # Sanitize filename and prevent directory traversal
    filename = os.path.basename(filename)
    if not filename.endswith('.log'):
        return 'Invalid filename', 400

    log_path = os.path.join(BASE_DIR, 'logs', filename)
    if os.path.exists(log_path):
        with open(log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    return 'Log file not found.', 404

@app.route('/api/sync-now', methods=['POST'])
@login_required
@verify_origin
def api_sync_now():
    try:
        config = load_config()
        log_file = setup_logging(config, prefix="sync")
        rotate_logs(keep=10)
        routine = SyncRoutine(config)
        threading.Thread(target=routine.run, daemon=True).start()
        return jsonify({
            "status": "Sync started",
            "log_file": os.path.basename(log_file)
        }), 202
    except Exception as e:
        logging.error(f"Failed to start sync: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/qb-credentials', methods=['GET', 'POST'])
@login_required
@verify_origin
def api_qb_credentials():
    """Get or set QuickBooks credentials in the .env file."""
    if request.method == 'POST':
        try:
            data = request.get_json()
            keys = ['QB_CLIENT_ID', 'QB_CLIENT_SECRET', 'QB_REFRESH_TOKEN', 'QB_REALM_ID', 'QB_ENVIRONMENT']
            for key in keys:
                if key in data and data[key]:
                    update_env_file(key, data[key])
            return jsonify({"status": "Success"})
        except Exception as e:
            logging.error(f"Failed to save QB credentials: {e}")
            return jsonify({"error": str(e)}), 500
    
    # GET: Return masked credentials
    return jsonify({
        "QB_CLIENT_ID": os.getenv("QB_CLIENT_ID", ""),
        "QB_CLIENT_SECRET": "********" if os.getenv("QB_CLIENT_SECRET") else "",
        "QB_REFRESH_TOKEN": "********" if os.getenv("QB_REFRESH_TOKEN") else "",
        "QB_REALM_ID": os.getenv("QB_REALM_ID", ""),
        "QB_ENVIRONMENT": os.getenv("QB_ENVIRONMENT", "sandbox")
    })

@app.route('/api/member-sync-settings', methods=['POST'])
@login_required
@verify_origin
def api_save_member_settings():
    """Consolidated endpoint to save all member sync configuration at once."""
    try:
        data = request.get_json()
        
        # 1. Update Schedule
        schedule = data.get('sync_frequency', '').strip()
        if schedule:
            # Validate the expression first
            cron_fields = parse_cron(schedule)
            if not cron_fields:
                return jsonify({"error": "Invalid schedule expression"}), 400
            update_env_file('SYNC_SCHEDULE', schedule)
            os.environ['SYNC_SCHEDULE'] = schedule
        else:
            update_env_file('SYNC_SCHEDULE', '')
            os.environ['SYNC_SCHEDULE'] = ''
        reschedule_sync()

        # 2. Update Email
        email = data.get('notification_email', '').strip()
        if email:
            update_env_file('SMTP_RECIPIENT_EMAIL', email)
            os.environ['SMTP_RECIPIENT_EMAIL'] = email

        # 3. Update PCO List ID
        pco_list_id = data.get('pco_list_id', '').strip()
        if pco_list_id:
            update_env_file('PCO_LIST_ID', pco_list_id)
            os.environ['PCO_LIST_ID'] = pco_list_id

        # 4. Update Display Name Format
        fmt = data.get('display_name_format', '').strip()
        if fmt:
            # Re-locate config path correctly
            config_path = os.path.join(BASE_DIR, 'config', 'config.json')
            if not os.path.exists(config_path):
                config_path = os.path.join(BASE_DIR, 'config.json')
            
            config = load_config(config_path)
            if 'planning_center' not in config:
                config['planning_center'] = {}
            if save_json_with_retries(config_path, config):
                logging.info(f"Successfully updated Display Name Format in {config_path}")
            else:
                logging.error(f"Failed to update Display Name Format in {config_path}")
                return jsonify({"error": "Failed to save configuration. Check folder permissions."}), 500

        return jsonify({"status": "Success"})
    except Exception as e:
        logging.error(f"Failed to save member settings: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/members')
@login_required
def api_members():
    """Returns all member sync history data."""
    history_path = os.path.join(BASE_DIR, 'data', 'member_sync_history.json')
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify(data)
        except Exception as e:
            logging.error(f"Error reading member_sync_history.json: {e}")
            return jsonify({"error": str(e)}), 500
    return jsonify({})

@app.route('/api/donations')
@login_required
def api_donations():
    """Returns all donation sync history data."""
    history_path = os.path.join(BASE_DIR, 'data', 'donation_sync_history.json')
    if os.path.exists(history_path):
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify(data)
        except Exception as e:
            logging.error(f"Error reading donation_sync_history.json: {e}")
            return jsonify({"error": str(e)}), 500
    return jsonify({})
    
@app.route('/api/logs/clear', methods=['POST'])
@login_required
@verify_origin
def api_clear_logs():
    """Clear historical Member or Donation sync logs from the logs directory."""
    try:
        data = request.get_json()
        log_type = data.get('type')  # 'member' or 'donation'
        
        logs_dir = os.path.join(BASE_DIR, 'logs')
        if not os.path.exists(logs_dir):
            return jsonify({"status": "Success", "message": "Logs directory does not exist"})
            
        count = 0
        for filename in os.listdir(logs_dir):
            # Filter for specific log types to avoid deleting the system log (server.log) or others
            if log_type == 'member' and filename.startswith('sync_') and filename.endswith('.log'):
                os.remove(os.path.join(logs_dir, filename))
                count += 1
            elif log_type == 'donation' and filename.startswith('donations_sync_') and filename.endswith('.log'):
                os.remove(os.path.join(logs_dir, filename))
                count += 1
        
        logging.info(f"Cleared {count} {log_type} sync logs.")
        return jsonify({"status": "Success", "cleared_count": count})
    except Exception as e:
        logging.error(f"Failed to clear logs: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/sync/clear-history', methods=['POST'])
@login_required
@verify_origin
def api_clear_history():
    """Clear persistent sync history files for Members or Donations."""
    try:
        data = request.get_json()
        sync_type = data.get('type')  # 'member' or 'donation'
        
        data_dir = os.path.join(BASE_DIR, 'data')
        files_to_remove = []
        
        if sync_type == 'member':
            files_to_remove = ['member_sync_history.json']
        elif sync_type == 'donation':
            files_to_remove = ['donation_sync_history.json', 'donation_sync_state.json']
            
        cleared = []
        for filename in files_to_remove:
            path = os.path.join(data_dir, filename)
            if os.path.exists(path):
                os.remove(path)
                cleared.append(filename)
                
        logging.info(f"Cleared {sync_type} sync history: {', '.join(cleared)}")
        return jsonify({"status": "Success", "cleared_files": cleared})
    except Exception as e:
        logging.error(f"Failed to clear sync history: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/member-history/<pc_id>')
@login_required
def api_member_history(pc_id):
    """Returns a unified timeline of member sync and donation sync events for a specific person."""
    combined_events = []
    
    # 1. Get Member Sync Events
    member_history_path = os.path.join(BASE_DIR, 'data', 'member_sync_history.json')
    if os.path.exists(member_history_path):
        try:
            with open(member_history_path, 'r', encoding='utf-8') as f:
                member_data = json.load(f)
                if pc_id in member_data:
                    for ev in member_data[pc_id].get('events', []):
                        event_copy = ev.copy()
                        event_copy['type'] = 'MEMBER'
                        combined_events.append(event_copy)
        except Exception: pass

    # 2. Get Donation Sync Events
    donation_history_path = os.path.join(BASE_DIR, 'data', 'donation_sync_history.json')
    if os.path.exists(donation_history_path):
        try:
            with open(donation_history_path, 'r', encoding='utf-8') as f:
                donation_data = json.load(f)
                # Scan all transaction entries for this pc_id
                for txn_id, txn_info in donation_data.items():
                    if txn_info.get('pco_id') == pc_id:
                        for ev in txn_info.get('events', []):
                            event_copy = ev.copy()
                            event_copy['type'] = 'DONATION'
                            # Add txn_id and detail context
                            event_copy['detail'] = f"QB Txn #{txn_id}: {event_copy.get('detail', '')}"
                            combined_events.append(event_copy)
        except Exception: pass

    # 3. Sort by date descending
    combined_events.sort(key=lambda x: x.get('date', ''), reverse=True)
    
    return jsonify({
        "pc_id": pc_id,
        "events": combined_events
    })

# ---------------------------------------------------------------------------
# Routes — Donation Sync API
# ---------------------------------------------------------------------------
@app.route('/api/donation-sync-now', methods=['POST'])
@login_required
@verify_origin
def api_donation_sync_now():
    """Trigger a manual donation reverse sync (QB → PCO Giving)."""
    try:
        # Check for lock file in data/ before starting
        lock_path = os.path.join(BASE_DIR, "data", "donation_sync.lock")
        if not os.path.exists(lock_path):
             # Fallback
             lock_path = os.path.join(BASE_DIR, "donation_sync.lock")
             
        if os.path.exists(lock_path):
            mtime = os.path.getmtime(lock_path)
            if (time.time() - mtime) < 3600:
                return jsonify({"status": "error", "message": "Donation sync already in progress"}), 409
        from sync_donations_qb_to_pc import DonationSyncRoutine
        # Use standardized config path
        config_path = os.path.join(BASE_DIR, 'config', 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(BASE_DIR, 'config.json')
        config = load_config(config_path)
        log_file = setup_logging(config, prefix="donations_sync")
        rotate_logs(keep=10)
        
        # Load donation settings for the manual sync
        settings_path = os.path.join(BASE_DIR, 'data', 'donation_sync_settings.json')
        donation_settings = {}
        if os.path.exists(settings_path):
            with open(settings_path, 'r') as f:
                donation_settings = json.load(f)

        # Save initial status
        status_path = os.path.join(BASE_DIR, 'data', 'latest_donation_sync_status.json')
        if not save_json_with_retries(status_path, {"status": "Running", "log_file": os.path.basename(log_file)}):
            logging.error(f"Failed to save initial donation sync status to {status_path}")

        routine = DonationSyncRoutine(config, donation_settings)
        threading.Thread(target=routine.run, daemon=True).start()
        
        return jsonify({
            "status": "Donation sync started",
            "log_file": os.path.basename(log_file)
        }), 202
    except Exception as e:
        logging.error(f"Failed to start donation sync: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/donation-sync-status')
@login_required
def api_donation_sync_status():
    """Return latest donation sync status and state."""
    status_path = os.path.join(BASE_DIR, 'data', 'latest_donation_sync_status.json')
    state_path = os.path.join(BASE_DIR, 'data', 'donation_sync_state.json')
    result = {"status": {}, "state": {}}

    if not os.path.exists(status_path):
        result["status"] = {"status": "Idle", "last_summary": {"donations_created": 0, "donations_skipped": 0, "errors": 0}}
    else:
        try:
            result["status"] = read_json_with_retries(status_path)
        except Exception:
            result["status"] = {"status": "Idle", "error": "Failed to read donation status file"}

    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_files = sorted(glob.glob(os.path.join(log_dir, "donations_sync_*.log")), key=os.path.getmtime, reverse=True)
    result["logs"] = [os.path.basename(f) for f in log_files]

    if os.path.exists(state_path):
        try:
            with open(state_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
                # Don't send the full list of synced_transaction_ids to the frontend
                result["state"] = {
                    "last_sync_time": state.get("last_sync_time"),
                    "synced_count": len(state.get("synced_transaction_ids", [])),
                    "last_summary": state.get("last_summary", {}),
                    "start_time": result["status"].get("start_time"),
                    "end_time": result["status"].get("end_time"),
                    "duration_seconds": result["status"].get("duration_seconds")
                }
        except Exception:
            pass

    # Add next run info for Donation Sync
    job = scheduler.get_job('donation_sync')
    if job and job.next_run_time:
        result["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        result["next_run"] = None

    return jsonify(result)

@app.route('/api/donation-sync-settings', methods=['GET'])
@login_required
def api_get_donation_settings():
    """Return current donation sync settings."""
    settings_path = os.path.join(BASE_DIR, 'data', 'donation_sync_settings.json')
    defaults = {
        "transaction_type": "SalesReceipt",
        "lookback_days": 30,
        "default_fund_name": "General Fund",
        "fund_mapping": {},
        "sync_frequency": "manual",
        "confirmation_email": "",
        "payment_method_map": {
            "Cash": "cash",
            "Check": "check",
            "Credit Card": "credit_card",
            "ACH": "ach"
        }
    }
    if os.path.exists(settings_path):
        try:
            with open(settings_path, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            defaults.update(saved)
        except Exception:
            pass
    # Add next run info for Donation Sync
    job = scheduler.get_job('donation_sync')
    if job and job.next_run_time:
        defaults["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        defaults["next_run"] = None

    return jsonify(defaults)

@app.route('/api/pco-funds', methods=['GET'])
@login_required
def api_get_pco_funds():
    """Fetch all funds from PC Giving."""
    try:
        app_id = os.getenv('PCO_APP_ID')
        pat = os.getenv('PCO_PAT')
        if not app_id or not pat:
            logging.error(f"PCO Credentials missing. APP_ID present: {bool(app_id)}, PAT present: {bool(pat)}")
            return jsonify({"error": "PCO_APP_ID or PCO_PAT missing in environment. Check Settings tab."}), 400
            
        client = PlanningCenterGivingClient()
        funds_list = client.get_all_funds() 
        return jsonify(funds_list)
    except Exception as e:
        logging.error(f"Failed to fetch PCO funds: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/qb-items', methods=['GET'])
@login_required
def api_get_qb_items():
    """Fetch all active items (products/services) from QuickBooks."""
    try:
        from sync_pc_to_qb import load_config, QuickBooksClient
        config = load_config()
        qb = QuickBooksClient(config.get('quickbooks', {}))
        items = qb.get_all_items()
        return jsonify(items)
    except Exception as e:
        logging.error(f"Failed to fetch QB items: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/donation-sync-settings', methods=['POST'])
@login_required
@verify_origin
def api_save_donation_settings():
    """Save donation sync settings from the web portal."""
    try:
        data = request.get_json()
        settings_path = os.path.join(BASE_DIR, 'data', 'donation_sync_settings.json')

        # Load existing settings
        existing = {}
        if os.path.exists(settings_path):
            try:
                with open(settings_path, 'r', encoding='utf-8') as f:
                    existing = json.load(f)
            except Exception:
                pass

        # Update with new values
        allowed_keys = [
            'transaction_type', 'lookback_days', 'default_fund_name',
            'product_service_map', 'payment_method_map', 'sync_frequency',
            'confirmation_email', 'auto_map_funds'
        ]
        for key in allowed_keys:
            if key in data:
                existing[key] = data[key]
            # Migration: if 'fund_mapping' is in data, use it for 'product_service_map'
            if key == 'product_service_map' and 'fund_mapping' in data:
                 existing['product_service_map'] = data['fund_mapping']

        if save_json_with_retries(settings_path, existing):
            logging.info(f"Successfully saved donation settings to {settings_path}")
        else:
            logging.error(f"Failed to save donation settings to {settings_path}")
            return jsonify({"error": "Failed to write settings file. Check folder permissions."}), 500

        # After saving, we should also reschedule the donation sync
        reschedule_donation_sync()

        return jsonify({"status": "Success", "settings": existing})
    except Exception as e:
        logging.error(f"Failed to save donation settings: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Routes — System Configuration
# ---------------------------------------------------------------------------

@app.route('/api/config', methods=['GET', 'POST'])
@login_required
@verify_origin
def api_config():
    """Get or update environment configuration."""
    # Ensure only authorized admins can access
    user = session.get('user', {})
    auth_settings = get_auth_settings() or {}
    is_sso = user.get('is_sso', False) or user.get('oid') is not None
    is_local_admin = not is_sso and user.get('preferred_username') == auth_settings.get('local_admin_user')
    
    if not (is_sso or is_local_admin):
        return jsonify({"error": "Admin privileges required."}), 403

    if request.method == 'POST':
        updates = request.json
        # Filter out masked values
        clean_updates = {}
        for k, v in updates.items():
            if '...' not in str(v) and '***' not in str(v):
                clean_updates[k] = v
        
        if not clean_updates:
            return jsonify({"success": True, "message": "No changes detected (all values were masked)."})
            
        if update_env_file_bulk(clean_updates):
            # Reload environment for the current process
            for k, v in clean_updates.items():
                os.environ[k] = str(v)
            logging.info(f"Updated .env with: {list(clean_updates.keys())}")
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Failed to save .env file."}), 500

    # GET: Return categorized config
    config = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, 'r') as f:
            for line in f:
                stripped = line.strip()
                if stripped and not stripped.startswith('#'):
                    parts = stripped.split('=', 1)
                    if len(parts) == 2:
                        key = parts[0].strip()
                        val = parts[1].strip().strip("'").strip('"')
                        config[key] = val
    
    # Categorize and add hints
    response_data = {}
    sections_map = {
        "pco": lambda k: k.startswith('PCO_'),
        "qb": lambda k: k.startswith('QB_'),
        "smtp": lambda k: k.startswith('SMTP_'),
        "azure": lambda k: k.startswith('AZURE_'),
        "general": lambda k: k not in SENSITIVE_KEYS and not k.startswith(('PCO_', 'QB_', 'SMTP_', 'AZURE_'))
    }

    # Helper to build field object
    def build_field(k, v):
        masked = mask_value(v) if k in SENSITIVE_KEYS else v
        return {
            "value": masked,
            "hint": CONFIG_HINTS.get(k, "")
        }

    for section_name, filter_func in sections_map.items():
        response_data[section_name] = {
            k: build_field(k, v) for k, v in config.items() if filter_func(k)
        }
    
    # Special cases for mixed keys
    if "SYNC_SCHEDULE" in config:
        if "SYNC_SCHEDULE" not in response_data["general"]:
            response_data["general"]["SYNC_SCHEDULE"] = build_field("SYNC_SCHEDULE", config["SYNC_SCHEDULE"])
    if "FLASK_SECRET_KEY" in config:
        response_data["general"]["FLASK_SECRET_KEY"] = build_field("FLASK_SECRET_KEY", config["FLASK_SECRET_KEY"])

    return jsonify(response_data)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    start_scheduler()
    app.run(host='0.0.0.0', port=8080, debug=False)
