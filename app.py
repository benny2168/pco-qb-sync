"""
PCO to QuickBooks Sync — Standalone Flask App
Replaces Azure Functions with a lightweight web server + scheduler.
"""
import os
import json
import glob
import logging
import threading

from flask import Flask, request, jsonify, send_file
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv, set_key

from sync_pc_to_qb import SyncRoutine, load_config, setup_logging, rotate_logs

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ENV_PATH = os.path.join(BASE_DIR, '.env')
load_dotenv(dotenv_path=ENV_PATH, override=True)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Scheduler Setup
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(daemon=True)

def parse_azure_cron(expr: str):
    """Convert a 6-field Azure CRON (sec min hour day month dow) to APScheduler fields."""
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
    schedule_expr = os.getenv('SYNC_SCHEDULE', '')
    if not schedule_expr:
        logging.warning("SYNC_SCHEDULE not set. No automatic sync will run.")
        return

    cron_fields = parse_azure_cron(schedule_expr)
    if not cron_fields:
        logging.error(f"Invalid SYNC_SCHEDULE expression: {schedule_expr}")
        return

    trigger = CronTrigger(**cron_fields)
    scheduler.add_job(run_scheduled_sync, trigger, id='pco_qb_sync', replace_existing=True)
    scheduler.start()
    logging.info(f"Scheduler started with SYNC_SCHEDULE = {schedule_expr}")

# ---------------------------------------------------------------------------
# Routes — Dashboard
# ---------------------------------------------------------------------------
@app.route('/dashboard')
def dashboard_page():
    dashboard_path = os.path.join(BASE_DIR, 'dashboard.html')
    if os.path.exists(dashboard_path):
        return send_file(dashboard_path, mimetype='text/html')
    return 'Dashboard file not found.', 404

@app.route('/')
def index():
    return dashboard_page()

# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------
@app.route('/api/status')
def api_status():
    # Re-read env for latest values
    load_dotenv(dotenv_path=ENV_PATH, override=True)

    status_path = os.path.join(BASE_DIR, 'latest_sync_status.json')
    status_data = {}
    if os.path.exists(status_path):
        try:
            with open(status_path, 'r', encoding='utf-8') as f:
                status_data = json.load(f)
        except Exception as e:
            logging.error(f"Error reading status json: {e}")
            status_data = {"error": "Failed to read status file"}

    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_files = sorted(glob.glob(os.path.join(log_dir, "sync_*.log")), key=os.path.getmtime, reverse=True)
    # Return just filenames (not full paths)
    log_basenames = [os.path.basename(f) for f in log_files]

    return jsonify({
        "status": status_data,
        "logs": log_basenames,
        "schedule": os.getenv("SYNC_SCHEDULE", "Not Set"),
        "recipient_email": os.getenv("SMTP_RECIPIENT_EMAIL", "")
    })

@app.route('/api/logs/<filename>')
def api_logs(filename):
    if not filename or not filename.endswith('.log') or '..' in filename:
        return 'Invalid filename', 400

    log_path = os.path.join(BASE_DIR, 'logs', filename)
    if os.path.exists(log_path):
        with open(log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    return 'Log file not found.', 404

@app.route('/api/sync-now', methods=['POST'])
def api_sync_now():
    try:
        config_path = os.path.join(BASE_DIR, 'config.json')
        config = load_config(config_path)
        setup_logging(config)
        rotate_logs(keep=10)
        routine = SyncRoutine(config)
        threading.Thread(target=routine.run, daemon=True).start()
        return jsonify({"status": "Sync started"}), 202
    except Exception as e:
        logging.error(f"Failed to start sync: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/config/email', methods=['POST'])
def api_save_email():
    try:
        data = request.get_json()
        email = data.get('email', '').strip()

        if email:
            set_key(ENV_PATH, 'SMTP_RECIPIENT_EMAIL', email)
            os.environ['SMTP_RECIPIENT_EMAIL'] = email
        else:
            set_key(ENV_PATH, 'SMTP_RECIPIENT_EMAIL', '')
            os.environ['SMTP_RECIPIENT_EMAIL'] = ''

        return jsonify({"status": "Success", "email": email})
    except Exception as e:
        logging.error(f"Failed to save email config: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    start_scheduler()
    app.run(host='0.0.0.0', port=8080, debug=False)
