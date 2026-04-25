from flask import Blueprint, request, jsonify, session, redirect, url_for, render_template
import os
import json
import logging
import secrets
from functools import wraps
from datetime import datetime

# Import utils
from utils import BASE_DIR, read_json_with_retries, robust_save_file, verify_origin

# Import PlanningCenterClient from the existing sync script
from sync_pc_to_qb import PlanningCenterClient, load_config

b_a_bp = Blueprint('b_a_reports', __name__, template_folder='templates')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('user'):
            if request.path.startswith('/api/'):
                return jsonify({"error": "Unauthorized"}), 401
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

CONFIG_PATH = os.path.join(BASE_DIR, 'data', 'b_a_config.json')

def get_b_a_config():
    """Load configuration for Birthdays and Anniversaries."""
    config = read_json_with_retries(CONFIG_PATH)
    if config:
        return config
    # Default configuration
    default_config = {
        "public_token": secrets.token_urlsafe(16),
        "lists": {
            # Format: "month_name": {"birthdays": "list_id", "anniversaries": "list_id"}
            str(m): {"birthdays": "", "anniversaries": ""} for m in range(1, 13)
        }
    }
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    robust_save_file(CONFIG_PATH, default_config)
    return default_config

def save_b_a_config(config):
    """Save configuration."""
    return robust_save_file(CONFIG_PATH, config)

def get_pc_client():
    """Helper to instantiate the PCO client."""
    config = load_config()
    pc_cfg = config.get('planning_center', {})
    if not pc_cfg:
        raise ValueError("Planning Center config missing in main config.json")
    return PlanningCenterClient(pc_cfg)

def _format_date(date_str):
    """Format YYYY-MM-DD to DD-MMM"""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%d-%b")
    except ValueError:
        return date_str

def fetch_list_details(list_id, pc_client):
    """Fetch all members in a PCO list and extract necessary fields."""
    if not list_id:
        return []
    
    # 1. Fetch the list results (Person IDs)
    url = f"{pc_client.base_url}/people/v2/lists/{list_id}/list_results"
    person_ids = []
    
    while url:
        logging.info(f"Fetching PC list results from {url}")
        import requests
        response = requests.get(url, auth=pc_client.auth)
        if response.status_code != 200:
            logging.error(f"Failed to fetch list {list_id}: {response.text}")
            break
            
        data = response.json()
        results = data.get('data', [])
        for res in results:
            try:
                person_id = res['relationships']['person']['data']['id']
                person_ids.append(person_id)
            except (KeyError, TypeError):
                pass
        
        url = data.get('links', {}).get('next')

    # 2. Make sure field definitions are loaded to map Prayer Group
    if not pc_client.field_definitions:
        pc_client.get_field_definitions()
    
    prayer_group_field_id = pc_client.field_definitions.get('prayer group')
    
    results_data = []
    
    # 3. Fetch person details. To be efficient, we should use the people endpoint with a list of IDs if possible.
    # However, PCO API usually requires fetching individually or using an include on the list_results.
    # Actually, list_results supports include=person. Let's optimize!
    # Let's write an optimized fetcher:
    url = f"{pc_client.base_url}/people/v2/lists/{list_id}/list_results?include=person,person.field_data"
    
    while url:
        import requests
        response = requests.get(url, auth=pc_client.auth)
        if response.status_code != 200:
            break
            
        data = response.json()
        included = data.get('included', [])
        
        # Build maps for included data
        people_map = {item['id']: item for item in included if item['type'] == 'Person'}
        field_data_map = {}
        for item in included:
            if item['type'] == 'FieldDatum':
                # Field data is linked to a person and a field definition
                # Unfortunately list_results include doesn't cleanly map field datum back to person unless we inspect relationships
                person_id = None
                # Let's inspect standard PCO API field datum relationships
                # Field Datum has a relationship to CustomFieldDefinition
                # Actually, person has relationships -> field_data. We can map from person to field data.
                pass
                
        # Simpler approach: if the list has < 100 people, we could fetch details.
        # But wait, include=person brings attributes like name, birthdate, anniversary.
        for res in data.get('data', []):
            try:
                person_id = res['relationships']['person']['data']['id']
                person = people_map.get(person_id)
                if not person:
                    continue
                    
                attrs = person.get('attributes', {})
                first_name = attrs.get('first_name', '')
                middle_name = attrs.get('middle_name', '')
                last_name = attrs.get('last_name', '')
                nickname = attrs.get('nickname', '')
                
                # Combine name: First Middle Last (Nickname)
                name_parts = [first_name]
                if middle_name:
                    name_parts.append(middle_name)
                if last_name:
                    name_parts.append(last_name)
                
                full_name = " ".join(name_parts)
                if nickname:
                    full_name += f" ({nickname})"
                    
                birthdate = _format_date(attrs.get('birthdate'))
                anniversary = _format_date(attrs.get('anniversary'))
                
                results_data.append({
                    "id": person_id,
                    "name": full_name,
                    "birthdate": birthdate,
                    "anniversary": anniversary,
                    "prayer_group": "" # Requires extra API call or robust parsing, let's leave it blank first, we will fetch it next
                })
            except Exception as e:
                logging.error(f"Error parsing person: {e}")
                
        url = data.get('links', {}).get('next')

    # Now we need Prayer Group. Since we need to fetch field_data efficiently, 
    # and the list might be long, we'll do individual calls for each person_id to ensure we get prayer group accurately.
    # PCO limits rate to 100 req/14 sec. We'll add a tiny sleep if needed.
    for p in results_data:
        try:
            p_details = pc_client.get_person_details(p["id"])
            p_included = p_details.get('included', [])
            pg_val = ""
            if prayer_group_field_id:
                for item in p_included:
                    if item.get('type') == 'FieldDatum':
                        fd_def_id = item.get('relationships', {}).get('field_definition', {}).get('data', {}).get('id')
                        if str(fd_def_id) == str(prayer_group_field_id):
                            pg_val = item.get('attributes', {}).get('value') or ""
                            break
            p["prayer_group"] = pg_val
        except Exception as e:
             logging.error(f"Error fetching details for person {p['id']}: {e}")
        
    return results_data

# ---------------------------------------------------------------------------
# Admin Routes
# ---------------------------------------------------------------------------
@b_a_bp.route('/dashboard')
@login_required
def dashboard():
    config = get_b_a_config()
    public_url = url_for('b_a_reports.public_page', token=config.get('public_token'), _external=True)
    return render_template('b_a_dashboard.html', user=session.get('user'), config=config, public_url=public_url)

@b_a_bp.route('/api/config', methods=['GET', 'POST'])
@login_required
@verify_origin
def api_config():
    config = get_b_a_config()
    if request.method == 'POST':
        data = request.json
        if 'lists' in data:
            config['lists'] = data['lists']
        if data.get('regenerate_token'):
            config['public_token'] = secrets.token_urlsafe(16)
            
        save_b_a_config(config)
        return jsonify({"success": True, "config": config})
        
    return jsonify(config)

@b_a_bp.route('/api/report', methods=['GET'])
@login_required
def api_report():
    month = request.args.get('month')
    if not month:
        return jsonify({"error": "Month parameter required"}), 400
        
    config = get_b_a_config()
    lists = config.get('lists', {}).get(str(month), {})
    
    bday_list_id = lists.get('birthdays')
    anniv_list_id = lists.get('anniversaries')
    
    try:
        pc_client = get_pc_client()
        birthdays = fetch_list_details(bday_list_id, pc_client) if bday_list_id else []
        anniversaries = fetch_list_details(anniv_list_id, pc_client) if anniv_list_id else []
        
        return jsonify({
            "birthdays": birthdays,
            "anniversaries": anniversaries
        })
    except Exception as e:
        logging.error(f"Failed to generate report: {e}")
        return jsonify({"error": str(e)}), 500

# ---------------------------------------------------------------------------
# Public Route
# ---------------------------------------------------------------------------
@b_a_bp.route('/public/<token>')
def public_page(token):
    config = get_b_a_config()
    if token != config.get('public_token'):
        return "Not Found", 404
        
    # We need to determine the current month to know which list to fetch
    current_month = datetime.now().month
    lists = config.get('lists', {}).get(str(current_month), {})
    
    bday_list_id = lists.get('birthdays')
    anniv_list_id = lists.get('anniversaries')
    
    birthdays = []
    anniversaries = []
    
    try:
        pc_client = get_pc_client()
        if bday_list_id:
            birthdays = fetch_list_details(bday_list_id, pc_client)
        if anniv_list_id:
            anniversaries = fetch_list_details(anniv_list_id, pc_client)
    except Exception as e:
        logging.error(f"Failed to fetch public report data: {e}")
        
    now = datetime.now()
    
    def parse_and_sort(items, date_key):
        # items format: {"name": "...", "birthdate": "15-May", ...}
        parsed = []
        for item in items:
            date_str = item.get(date_key, "")
            if not date_str: continue
            try:
                # "15-May" format
                dt = datetime.strptime(date_str, "%d-%b")
                # Assign current year to be able to compare
                item_date = dt.replace(year=now.year)
                item['_date_obj'] = item_date
                parsed.append(item)
            except ValueError:
                pass
        # sort by month and day
        parsed.sort(key=lambda x: (x['_date_obj'].month, x['_date_obj'].day))
        return parsed

    birthdays = parse_and_sort(birthdays, "birthdate")
    anniversaries = parse_and_sort(anniversaries, "anniversary")

    def categorize(items):
        today, this_week, this_month = [], [], []
        # 'this week' is defined as next 7 days or current ISO calendar week? Let's use next 7 days for simplicity, or same calendar week.
        # User said "this day, week and month". We will use current iso calendar week.
        current_year, current_week, _ = now.isocalendar()
        for item in items:
            item_date = item['_date_obj']
            # Only include if it's the current month (just in case list has others)
            if item_date.month == now.month:
                this_month.append(item)
                
                if item_date.day == now.day:
                    today.append(item)
                elif item_date.isocalendar()[1] == current_week:
                    this_week.append(item)
        return today, this_week, this_month

    b_today, b_week, b_month = categorize(birthdays)
    a_today, a_week, a_month = categorize(anniversaries)
    
    return render_template('public_b_a.html', 
        b_today=b_today, b_week=b_week, b_month=b_month,
        a_today=a_today, a_week=a_week, a_month=a_month,
        current_date=now.strftime("%B %d, %Y")
    )
