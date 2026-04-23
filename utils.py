import os
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
from flask import request

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")
if not os.path.isfile(ENV_PATH):
    fallback_path = os.path.join(BASE_DIR, ".env")
    if os.path.isfile(fallback_path):
        ENV_PATH = fallback_path

AUTH_SETTINGS_PATH = os.path.join(BASE_DIR, "data", "auth_settings.json")
ADMIN_LOGINS_PATH = os.path.join(BASE_DIR, "data", "admin_logins.json")

def read_json_with_retries(path, retries=5, delay=0.1):
    for i in range(retries):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except OSError as e:
            if e.errno == 35 and i < retries - 1:
                time.sleep(delay)
                continue
            raise
        except json.JSONDecodeError:
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise
        except FileNotFoundError:
            return None
    return None

def robust_save_file(path, content, is_json=True, retries=5, delay=0.1):
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
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        if is_json:
                            json.dump(content, f, indent=4)
                        else:
                            f.write(content)
                    return True
                except:
                    return False
            if i < retries - 1:
                time.sleep(delay)
                continue
            return False
    return False

def get_auth_settings():
    settings = read_json_with_retries(AUTH_SETTINGS_PATH)
    if settings:
        return settings
    default_settings = {
        "local_admin_user": "admin",
        "local_admin_password_hash": generate_password_hash("admin1234"),
        "local_login_enabled": True
    }
    os.makedirs(os.path.dirname(AUTH_SETTINGS_PATH), exist_ok=True)
    robust_save_file(AUTH_SETTINGS_PATH, default_settings)
    return default_settings

def log_admin_login(user_info):
    try:
        log_entry = {
            'username': user_info.get('preferred_username') or user_info.get('name') or 'Unknown User',
            'timestamp': datetime.now().isoformat(),
            'ip': request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip(),
            'is_sso': user_info.get('is_sso', False)
        }
        logs = read_json_with_retries(ADMIN_LOGINS_PATH) or []
        logs.insert(0, log_entry)
        logs = logs[:100]
        os.makedirs(os.path.dirname(ADMIN_LOGINS_PATH), exist_ok=True)
        robust_save_file(ADMIN_LOGINS_PATH, logs)
    except Exception as e:
        logging.error(f"Failed to log admin login: {e}")

def update_env_file_bulk(updates):
    if not os.path.exists(ENV_PATH):
        with open(ENV_PATH, 'w') as f: pass
    with open(ENV_PATH, 'r') as f:
        lines = f.readlines()
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
    for key, value in updates.items():
        if key not in processed_keys:
            new_lines.append(f"{key}='{value}'\n")
    if robust_save_file(ENV_PATH, "".join(new_lines), is_json=False):
        load_dotenv(dotenv_path=ENV_PATH, override=True)
        return True
    return False

from flask import request, jsonify
from functools import wraps

def verify_origin(f):
    """Basic CSRF mitigation: verify request origin matches host."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == 'POST':
            origin = request.headers.get('Origin')
            host = request.headers.get('Host')
            if origin and host not in origin:
                 logging.warning(f"CSRF Alert: Origin '{origin}' does not match Host '{host}'")
                 return jsonify({"error": "Forbidden: CSRF protection triggered."}), 403
        return f(*args, **kwargs)
    return decorated_function
