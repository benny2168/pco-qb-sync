"""
Admin Portal - Modular Web Server
Hosts multiple administrative tools (PCO-QB Sync, Birthdays, Tasks) via Flask Blueprints.
"""
import os
import json
import logging
import time
from datetime import datetime
from functools import wraps
import secrets

from flask import Flask, request, jsonify, session, redirect, url_for, render_template
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from authlib.integrations.flask_client import OAuth

# ---------------------------------------------------------------------------
# Bootstrap Config
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ENV_PATH = os.path.join(BASE_DIR, 'config', '.env')
if not os.path.isfile(ENV_PATH):
    fallback_path = os.path.join(BASE_DIR, '.env')
    if os.path.isfile(fallback_path):
        ENV_PATH = fallback_path

load_dotenv(dotenv_path=ENV_PATH, override=True)
logging.basicConfig(level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper()),
                    format='%(asctime)s - %(levelname)s - %(message)s')

AUTH_SETTINGS_PATH = os.path.join(BASE_DIR, 'data', 'auth_settings.json')
ADMIN_LOGINS_PATH = os.path.join(BASE_DIR, 'data', 'admin_logins.json')

# ---------------------------------------------------------------------------
# Core Utilities
# ---------------------------------------------------------------------------
from utils import *
# ---------------------------------------------------------------------------
# App Initialization
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder='static', static_url_path='/static')
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

if not os.getenv('FLASK_SECRET_KEY'):
    new_key = secrets.token_hex(32)
    update_env_file_bulk({'FLASK_SECRET_KEY': new_key})
    os.environ['FLASK_SECRET_KEY'] = new_key

app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')
app.config['SESSION_COOKIE_NAME'] = 'admin_portal_session'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

@app.after_request
def add_security_headers(response):
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response

# ---------------------------------------------------------------------------
# OAuth (Authentik) Setup
# ---------------------------------------------------------------------------
oauth = OAuth(app)
authentik = None

OIDC_CLIENT_ID = os.getenv('OIDC_CLIENT_ID')
OIDC_CLIENT_SECRET = os.getenv('OIDC_CLIENT_SECRET')
OIDC_DISCOVERY_URL = os.getenv('OIDC_DISCOVERY_URL')

if OIDC_CLIENT_ID and OIDC_CLIENT_SECRET and OIDC_DISCOVERY_URL:
    authentik = oauth.register(
        name='authentik',
        client_id=OIDC_CLIENT_ID,
        client_secret=OIDC_CLIENT_SECRET,
        server_metadata_url=OIDC_DISCOVERY_URL,
        client_kwargs={
            'scope': 'openid profile email groups',
            'jwks_algorithms': ['RS256']
        }
    )
    logging.info("Registered Authentik OIDC client.")
else:
    logging.warning("Authentik OIDC variables missing. SSO login will be disabled.")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user'):
            if request.path.startswith('/api/'):
                return jsonify({"error": "Unauthorized"}), 401
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

# ---------------------------------------------------------------------------
# Scheduler Setup
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(daemon=True)
if not scheduler.running:
    scheduler.start()
    logging.info("Global APScheduler started.")

# ---------------------------------------------------------------------------
# Blueprint Registration
# ---------------------------------------------------------------------------
from modules.pco_qb_sync.routes import pco_qb_bp, register_scheduler_jobs

app.register_blueprint(pco_qb_bp, url_prefix='/pco-qb')
register_scheduler_jobs(scheduler, BASE_DIR)

# ---------------------------------------------------------------------------
# Routes — Auth & Main
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    if not session.get('user'):
        auth_settings = get_auth_settings() or {}
        local_enabled = auth_settings.get('local_login_enabled', True)
        return render_template('login.html', local_enabled=local_enabled, sso_enabled=bool(authentik))
    return redirect(url_for('dashboard_page'))

@app.route("/login")
def login():
    if not authentik:
        return "SSO is not configured.", 500
    redirect_uri = url_for('authorized', _external=True)
    # Allows reverse proxy setups to override protocol
    if request.headers.get('X-Forwarded-Proto') == 'https':
        redirect_uri = redirect_uri.replace('http://', 'https://')
    return authentik.authorize_redirect(redirect_uri)

@app.route("/callback")
def authorized():
    if not authentik:
        return "SSO is not configured.", 500
    try:
        token = authentik.authorize_access_token()
        user_info = token.get('userinfo')
        if not user_info:
            return "Login failure: No user info received.", 401
            
        user_claims = dict(user_info)
        user_claims["is_sso"] = True
        # Normalize email/username — Authentik may put it in 'email' or 'preferred_username'
        email = user_claims.get("email") or user_claims.get("preferred_username", "")
        user_claims["preferred_username"] = email
        # Ensure 'name' is always set — fall back to email if not provided
        if not user_claims.get("name"):
            user_claims["name"] = user_claims.get("given_name", "") + " " + user_claims.get("family_name", "")
            user_claims["name"] = user_claims["name"].strip() or email
        session["user"] = user_claims
        log_admin_login(session['user'])
        logging.info(f"User {session['user'].get('preferred_username')} logged in successfully via SSO.")
        return redirect(url_for("dashboard_page"))
    except Exception as e:
        logging.exception("Authorized callback error")
        return f"Internal authentication error: {e}", 500

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
                'is_sso': False,
                'groups': ['admin']
            }
            log_admin_login(session['user'])
            return redirect(url_for('dashboard_page'))
        else:
            error = "Invalid username or password."
            
    return render_template('local_login.html', error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/dashboard')
@login_required
def dashboard_page():
    # Pass user groups to render module cards conditionally
    user_groups = session.get('user', {}).get('groups', [])
    if isinstance(user_groups, str):
        user_groups = [user_groups]
        
    modules = [
        {
            "id": "pco-qb",
            "name": "PCO ↔ QB Sync",
            "icon": "activity",
            "url": url_for('pco_qb.dashboard_page'),
            "description": "Synchronize Planning Center Members & Donations with QuickBooks Online",
            "allowed": True # Or conditionally based on groups
        }
        # Add future modules here
    ]
    return render_template('portal_dashboard.html', user=session.get('user'), modules=modules)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7365, debug=True)
