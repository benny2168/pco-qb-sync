import os
import json
import logging
import time
import requests
import smtplib
import glob
import re
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file (priority: config/.env)
dotenv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    load_dotenv()

# Load configuration
def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    if not config_path:
        config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'config.json')
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# Logs helper
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(config: Dict[str, Any], prefix: str = "sync"):
    log_cfg = config.get('logging', {})
    
    # Generate timestamped filename inside logs/ directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"{prefix}_{timestamp}.log")
    
    logging.basicConfig(
        level=getattr(logging, log_cfg.get('level', 'INFO')),
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='a',
        force=True
    )
    # Also log to console
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, log_cfg.get('level', 'INFO')))
    logging.getLogger('').addHandler(console)
    return log_file

def rotate_logs(keep: int = 10, prefix: str = "sync"):
    """Keep only the 'keep' most recent sync log files with a given prefix."""
    # Ensure prefix ends with _ to avoid partial matches
    pattern = f"{prefix}_*.log"
    log_files = sorted(glob.glob(os.path.join(LOG_DIR, pattern)), key=os.path.getmtime, reverse=True)
    if len(log_files) > keep:
        logs_to_delete = log_files[keep:]
        for log_file in logs_to_delete:
            try:
                os.remove(log_file)
            except Exception:
                pass

class PlanningCenterClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config.get('base_url')
        self.head_of_household_list_id = os.getenv('PCO_LIST_ID') or config.get('head_of_household_list_id')
        
        if not self.base_url:
            raise KeyError("Planning Center 'base_url' missing in config.json")
        if not self.head_of_household_list_id:
            logging.warning("PCO 'PCO_LIST_ID' (env) or 'head_of_household_list_id' (config) missing")

        self.app_id = os.getenv('PCO_APP_ID')
        self.token = os.getenv('PCO_PAT')
        
        if not self.app_id or not self.token:
            raise ValueError("PCO_APP_ID or PCO_PAT missing in .env")
            
        self.auth = (self.app_id, self.token)
        self.field_definitions = {}

    def get_field_definitions(self) -> None:
        """Fetch all custom field definitions from PCO and map them locally."""
        url = f"{self.base_url}/people/v2/field_definitions"
        while url:
            logging.debug(f"Fetching PC field definitions from {url}")
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            data = response.json()
            for item in data.get('data', []):
                name = item['attributes'].get('name')
                if name:
                    self.field_definitions[name.lower()] = item['id']
            url = data.get('links', {}).get('next')
        logging.info(f"Loaded {len(self.field_definitions)} custom field definitions from PCO.")

    def get_list_results(self) -> List[str]:
        """Fetch person IDs in the Head of Household list."""
        url = f"{self.base_url}/people/v2/lists/{self.head_of_household_list_id}/list_results"
        person_ids = []
        
        while url:
            logging.info(f"Fetching PC list results from {url}")
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            data = response.json()
            
            # Debug: Log raw response
            logging.debug("Raw PC List Response:")
            logging.debug(json.dumps(data, indent=2))
            
            results = data.get('data', [])
            for res in results:
                try:
                    person_id = res['relationships']['person']['data']['id']
                    person_ids.append(person_id)
                except (KeyError, TypeError) as e:
                    logging.warning(f"Could not extract person ID from list result: {e}")
            
            url = data.get('links', {}).get('next')
            
        return person_ids

    def get_person_details(self, person_id: str) -> Dict[str, Any]:
        """Fetch detailed person object including address, emails, and custom field data."""
        url = f"{self.base_url}/people/v2/people/{person_id}"
        params = {'include': 'emails,addresses,phone_numbers,field_data,name_prefix,name_suffix'}
        response = requests.get(url, auth=self.auth, params=params)
        response.raise_for_status()
        return response.json()

class QuickBooksClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config.get('base_url')
        if not self.base_url:
            raise KeyError("QuickBooks 'base_url' missing in config.json")

        self.realm_id = os.getenv('QB_REALM_ID')
        self.client_id = os.getenv('QB_CLIENT_ID')
        self.client_secret = os.getenv('QB_CLIENT_SECRET')
        self.refresh_token = os.getenv('QB_REFRESH_TOKEN')
        
        if not all([self.realm_id, self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError("QB credentials missing in .env")

        self.access_token = None
        self.custom_fields_map = config['custom_fields']
        self.minorversion = 70
        self.discovered_definitions = {} # Name -> DefinitionId
        self.active_custom_field_names = set() 
        # Fallback for QBO Plus / Sandbox if discovery fails
        self.custom_fields_fallback = {
            config['custom_fields'].get('pc_id', 'External ID'): '1',
            config['custom_fields'].get('nickname', 'Nickname'): '2',
            config['custom_fields'].get('prayer_group', 'PrayerGroup'): '3'
        }

    def _refresh_access_token(self):
        """Refresh OAuth 2.0 token."""
        token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token
        }
        logging.debug("Refreshing QuickBooks access token...")
        response = requests.post(token_url, data=payload, auth=(self.client_id, self.client_secret))
        
        if not response.ok:
            logging.error(f"QB Token Refresh failed with status {response.status_code}")
            logging.error(f"QB Token Refresh Error: {response.text}")
            response.raise_for_status()
            
        data = response.json()
        self.access_token = data['access_token']
        if 'refresh_token' in data:
            self.refresh_token = data['refresh_token']
            
            # Save the new refresh token back to .env
            env_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', '.env')
            if not os.path.exists(env_path):
                env_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
            
            if os.path.exists(env_path):
                try:
                    # Manual update to avoid atomic rename (which fails in Docker single-file volumes)
                    with open(env_path, 'r') as f:
                        lines = f.readlines()
                    with open(env_path, 'w') as f:
                        found = False
                        for line in lines:
                            if line.startswith('QB_REFRESH_TOKEN='):
                                f.write(f"QB_REFRESH_TOKEN='{self.refresh_token}'\n")
                                found = True
                            else:
                                f.write(line)
                        if not found:
                            f.write(f"QB_REFRESH_TOKEN='{self.refresh_token}'\n")
                    logging.info("New QB refresh token automatically saved to .env")
                except Exception as e:
                    logging.error(f"Failed to auto-save refresh token: {e}")
            else:
                logging.warning(".env file not found. Could not save the new refresh token!")

    def _get_headers(self):
        if not self.access_token:
            self._refresh_access_token()
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_custom_field_definitions(self):
        """Fetch custom field definitions from both Preferences and CustomField APIs."""
        logging.info("QuickBooksClient: Fetching custom field definitions...")
        
        # 1. Fetch from Preferences (Classic Custom Fields)
        url_prefs = f"{self.base_url}/v3/company/{self.realm_id}/preferences"
        params = {'minorversion': self.minorversion}
        try:
            resp_prefs = requests.get(url_prefs, headers=self._get_headers(), params=params)
            resp_prefs.raise_for_status()
            data_prefs = resp_prefs.json()
            prefs = data_prefs.get('Preferences', {})
            sales_prefs = prefs.get('SalesFormsPrefs', {})
            custom_fields = sales_prefs.get('CustomField', [])
            
            # Flatten classic structure
            for cf_wrap in custom_fields:
                inner_fields = cf_wrap.get('CustomField', [])
                for cf in inner_fields:
                    name = cf.get('Name')
                    # Classic names follow pattern SalesFormsPrefs.UseSalesCustom[1-3]
                    if name and 'UseSalesCustom' in name:
                        # Extract the number (1, 2, or 3)
                        idx_match = re.search(r'UseSalesCustom(\d+)', name)
                        if idx_match:
                            idx = idx_match.group(1)
                            # The DefinitionId for classic fields is usually just the index
                            self.discovered_definitions[name] = idx
                            # We don't have the label here yet, but we'll try to match by label later
                            logging.debug(f"Found Classic Custom Field: {name} (ID: {idx})")

        except Exception as e:
            logging.warning(f"Failed to fetch classic custom fields from Preferences: {e}")

        # 2. Fetch from CustomField API (Enhanced Custom Fields - QuickBooks Advanced/Standard)
        # This endpoint is available in minorversion 54+
        url_cf = f"{self.base_url}/v3/company/{self.realm_id}/customfield"
        try:
            resp_cf = requests.get(url_cf, headers=self._get_headers(), params=params)
            # This endpoint might return 404/400 if not supported or no fields exist
            if resp_cf.ok:
                data_cf = resp_cf.json()
                # The response is usually a list of CustomField objects
                # Or sometimes wrapped in a QueryResponse
                cf_list = data_cf.get('CustomField', [])
                if not cf_list and 'QueryResponse' in data_cf:
                    cf_list = data_cf['QueryResponse'].get('CustomField', [])
                
                for cf in cf_list:
                    name = cf.get('Name')
                    def_id = cf.get('Id') # Enhanced fields use 'Id'
                    is_active = cf.get('Active', True)
                    
                    if name and is_active:
                        self.discovered_definitions[name] = def_id
                        self.active_custom_field_names.add(name)
                        logging.info(f"Discovered Enhanced Custom Field: '{name}' (ID: {def_id})")
            else:
                logging.debug(f"CustomField endpoint not supported or failed (Status {resp_cf.status_code})")
        except Exception as e:
            logging.debug(f"Failed to fetch enhanced custom fields: {e}")

        # Summary
        if self.discovered_definitions:
            logging.info(f"Total QuickBooks Custom Fields Discovery: {list(self.discovered_definitions.keys())}")
        else:
            logging.info(f"Discovered QB Custom Fields: {self.discovered_definitions}")

    def get_all_accounts(self) -> List[Dict[str, str]]:
        """Fetch all active income and asset accounts from QB."""
        accounts = []
        start_position = 1
        max_results = 100
        
        while True:
            # Query for Income, Other Current Asset (typically used for restricted funds)
            query = (
                f"SELECT Id, Name, AccountType FROM Account "
                f"WHERE Active = true AND AccountType IN ('Income', 'Other Current Asset') "
                f"STARTPOSITION {start_position} MAXRESULTS {max_results}"
            )
            url = f"{self.base_url}/v3/company/{self.realm_id}/query"
            params = {"query": query, "minorversion": self.minorversion}
            
            response = requests.get(url, headers=self._get_headers(), params=params)
            if response.status_code == 401:
                self._refresh_access_token()
                response = requests.get(url, headers=self._get_headers(), params=params)
            
            response.raise_for_status()
            data = response.json().get('QueryResponse', {})
            batch = data.get('Account', [])
            for acc in batch:
                accounts.append({
                    "id": acc["Id"],
                    "name": acc["Name"],
                    "type": acc["AccountType"]
                })
            
            if len(batch) < max_results:
                break
            start_position += max_results
            
        logging.info(f"Loaded {len(accounts)} active accounts from QB.")
        return accounts

    def get_all_items(self) -> List[Dict[str, str]]:
        """Fetch all active products/services (Items) from QB."""
        items = []
        start_position = 1
        max_results = 100
        
        while True:
            query = (
                f"SELECT Id, Name, Type FROM Item "
                f"WHERE Active = true "
                f"STARTPOSITION {start_position} MAXRESULTS {max_results}"
            )
            url = f"{self.base_url}/v3/company/{self.realm_id}/query"
            params = {"query": query, "minorversion": self.minorversion}
            
            response = requests.get(url, headers=self._get_headers(), params=params)
            if response.status_code == 401:
                self._refresh_access_token()
                response = requests.get(url, headers=self._get_headers(), params=params)
            
            response.raise_for_status()
            data = response.json().get('QueryResponse', {})
            batch = data.get('Item', [])
            for itm in batch:
                items.append({
                    "id": itm["Id"],
                    "name": itm["Name"],
                    "type": itm.get("Type", "Unknown")
                })
                
            if len(batch) < max_results:
                break
            start_position += max_results
            
        logging.info(f"Loaded {len(items)} active items from QB.")
        return items

    def get_all_customers(self) -> List[Dict[str, Any]]:
        """Fetch all customers from QB to build a local lookup map."""
        customers = []
        start_position = 1
        max_results = 100
        
        while True:
            # Note: CustomField is supposed to be returned by SELECT * but some sandbox/minorversions 
            # might be finicky. Being explicit helps in some cases.
            query = f"SELECT * FROM Customer STARTPOSITION {start_position} MAXRESULTS {max_results}"
            url = f"{self.base_url}/v3/company/{self.realm_id}/query"
            logging.debug(f"Fetching QB Customers: {query}")
            params = {'query': query, 'minorversion': self.minorversion}
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json().get('QueryResponse', {})
            batch = data.get('Customer', [])
            customers.extend(batch)
            
            if len(batch) < max_results:
                break
            start_position += max_results
            
        return customers

    def get_customer(self, customer_id: str) -> Dict[str, Any]:
        """Fetch a single customer by ID with custom fields included."""
        url = f"{self.base_url}/v3/company/{self.realm_id}/customer/{customer_id}"
        # include=customfields is sometimes needed for Enhanced Custom Fields to appear
        params = {'minorversion': self.minorversion, 'include': 'customfields'}
        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        return response.json().get('Customer', {})

    def create_customer(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/v3/company/{self.realm_id}/customer"
        params = {'minorversion': self.minorversion}
        response = requests.post(url, headers=self._get_headers(), json=customer_data, params=params)
        if not response.ok:
            logging.error(f"QB Create Customer failed: {response.status_code} - {response.text}")
            response.raise_for_status()
        return response.json().get('Customer', {})

    def update_customer(self, customer_id: str, sync_token: str, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/v3/company/{self.realm_id}/customer"
        params = {'minorversion': self.minorversion}
        # Create a copy to avoid mutating the original payload in case of retries
        payload = customer_data.copy()
        payload['Id'] = customer_id
        payload['SyncToken'] = sync_token
        payload['sparse'] = True
        
        logging.debug(f"QB Update Payload for {customer_id}: {json.dumps(payload, indent=2)}")
        response = requests.post(url, headers=self._get_headers(), json=payload, params=params)
        
        if not response.ok:
            logging.error(f"QB Update failed for {customer_id}: {response.status_code} - {response.text}")
            # We raise here, the caller handles Stale Object (5010) specifically
            response.raise_for_status()
            
        result = response.json().get('Customer', {})
        logging.debug(f"QB Update Response for {customer_id}: DisplayName='{result.get('DisplayName')}', SyncToken='{result.get('SyncToken')}'")
        return result

class SyncRoutine:
    def __init__(self, config: Dict[str, Any]):
        # Dynamically reload environment variables on every start
        # so manual .env token updates take effect without restarting the app.
        env_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', '.env')
        if not os.path.exists(env_path):
            env_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '.env')
        load_dotenv(dotenv_path=env_path, override=True)

        self.config = config
        
        planning_center_cfg = config.get('planning_center')
        if not planning_center_cfg:
            raise KeyError("'planning_center' section missing in config.json")
        self.pc = PlanningCenterClient(planning_center_cfg)
        
        quickbooks_cfg = config.get('quickbooks')
        if not quickbooks_cfg:
            raise KeyError("'quickbooks' section missing in config.json")
        self.qb = QuickBooksClient(quickbooks_cfg)
        self.summary = {
            'status': 'Running',
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration_seconds': 0,
            'created': 0,
            'updated': 0,
            'errors': 0,
            'logs': []
        }
        
        # Member sync history tracker in data/
        self.history_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'member_sync_history.json')
        self.member_history = self._load_member_history()

    def _load_member_history(self) -> Dict[str, Any]:
        """Load existing member sync history from disk."""
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Could not load member_sync_history.json: {e}")
        return {}

    def _save_member_history(self, retries=5, delay=0.1):
        """Persist member history to disk using atomic rename with retries."""
        for i in range(retries):
            try:
                temp_path = self.history_path + ".tmp"
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.member_history, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, self.history_path)
                return True
            except OSError as e:
                if e.errno == 35: # Resource deadlock avoided
                    if i < retries - 1:
                        time.sleep(delay)
                        continue
                logging.error(f"Failed to save member_sync_history.json (attempt {i+1}): {e}")
                if i == retries - 1: raise
            except Exception as e:
                logging.error(f"Unexpected error saving member history: {e}")
                break
        return False

    def _record_member_event(self, pc_id: str, name: str, action: str, detail: str = "", changes: Optional[List[Dict[str, Any]]] = None, display_name: Optional[str] = None):
        """Append an event to a member's sync history."""
        if pc_id not in self.member_history:
            self.member_history[pc_id] = {'name': name, 'events': []}
        # Always update name to latest
        self.member_history[pc_id]['name'] = name
        
        event = {
            'date': datetime.now().isoformat(),
            'action': action,
            'detail': detail
        }
        if display_name:
            event['display_name'] = display_name
            
        if changes:
            event['changes'] = changes
            
        self.member_history[pc_id]['events'].append(event)
        # Keep last 100 events per member to prevent unbounded growth
        self.member_history[pc_id]['events'] = self.member_history[pc_id]['events'][-100:]

    def _save_summary_json(self, retries=5, delay=0.1):
        """Save the latest summary to data/latest_sync_status.json using atomic rename with retries."""
        status_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', 'latest_sync_status.json')
        temp_path = status_path + ".tmp"
        
        for i in range(retries):
            try:
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump({"status": self.summary["status"], "last_summary": self.summary}, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, status_path)
                return True
            except OSError as e:
                if e.errno == 35: # Resource deadlock avoided
                    if i < retries - 1:
                        time.sleep(delay)
                        continue
                logging.error(f"Failed to save status JSON (attempt {i+1}): {e}")
            except Exception as e:
                logging.error(f"Unexpected error saving status JSON: {e}")
                break
        return False

    def _log_record(self, action: str, person_name: str, detail: str = ""):
        msg = f"{action}: {person_name} - {detail}"
        logging.info(msg)
        self.summary['logs'].append(msg)

    def _get_pc_id_from_qb_customer(self, qb_customer: Dict[str, Any]) -> Optional[str]:
        """Extract Planning Center ID from QB Fax field."""
        fax = qb_customer.get('Fax', {})
        pc_id = fax.get('FreeFormNumber')
        if pc_id:
            logging.debug(f"Found PC ID {pc_id} in Fax field for {qb_customer.get('DisplayName')}")
            return pc_id
        return None

    def _map_pc_to_qb(self, detailed_pc: Dict[str, Any]) -> Dict[str, Any]:
        """Map PC person details to QB customer payload, using Fax for PC ID."""
        person_data = detailed_pc['data']
        attrs = person_data['attributes']
        included = detailed_pc.get('included', [])
        
        # Extract primary email
        emails = [item['attributes']['address'] for item in included if item['type'] == 'Email' and item['attributes'].get('primary')]
        primary_email = emails[0] if emails else ""

        # Extract primary phone
        phones = [item['attributes']['number'] for item in included if item['type'] == 'PhoneNumber' and item['attributes'].get('primary')]
        primary_phone = phones[0] if phones else ""

        # Extract address
        addresses = [item['attributes'] for item in included if item['type'] == 'Address' and item['attributes'].get('primary')]
        addr = addresses[0] if addresses else {}

        # Extract Title (name_prefix) and Suffix (name_suffix) from included relationships
        name_prefix = next((item['attributes']['value'] for item in included if item['type'] == 'NamePrefix'), None)
        name_suffix = next((item['attributes']['value'] for item in included if item['type'] == 'NameSuffix'), None)
        nickname = attrs.get('nickname')
        
        # Calculate dynamic display name
        display_fmt = self.config.get('planning_center', {}).get('display_name_format', '{first_name} {last_name}')
        
        # Mapping for display name calculation
        # Extract Prayer Group first (re-using logic from later in function for consistency)
        pco_pg_id = self.pc.field_definitions.get('prayer group')
        prayer_group_val = ""
        if pco_pg_id:
            for fd in [item for item in included if item.get('type') == 'FieldDatum']:
                if fd.get('relationships', {}).get('field_definition', {}).get('data', {}).get('id') == pco_pg_id:
                    prayer_group_val = fd.get('attributes', {}).get('value') or ""
                    break

        format_map = {
            'first_name': attrs.get('first_name') or "",
            'middle_name': attrs.get('middle_name') or "",
            'last_name': attrs.get('last_name') or "",
            'nickname': nickname or "",
            'prayer_group': prayer_group_val or "",
            'title': name_prefix or "",
            'suffix': name_suffix or ""
        }
        
        display_name = display_fmt
        
        # 1. Handle optional blocks [...]
        # Find all content inside square brackets
        optional_blocks = re.findall(r'\[([^\]]+)\]', display_name)
        for block_content in optional_blocks:
            full_block = f"[{block_content}]"
            # Extract all {tags} within this block
            tags_in_block = re.findall(r'\{([^\}]+)\}', block_content)
            
            # Check if ANY of the tags in this block have a non-empty value
            has_value = False
            for tag in tags_in_block:
                if format_map.get(tag):
                    has_value = True
                    break
            
            if has_value:
                # Keep the block content (minus the brackets) and replace tags
                processed_block = block_content
                for tag in tags_in_block:
                    val = format_map.get(tag, "")
                    processed_block = processed_block.replace(f"{{{tag}}}", val)
                display_name = display_name.replace(full_block, processed_block)
            else:
                # Remove the entire block
                display_name = display_name.replace(full_block, "")

        # 2. Handle remaining standard tags outside of brackets
        for key, val in format_map.items():
            display_name = display_name.replace(f"{{{key}}}", val)
            
        # Clean up extra spaces
        display_name = re.sub(r'\s+', ' ', display_name).strip()
        
        logging.debug(f"PCO Name Components: Prefix={name_prefix}, First={attrs.get('first_name')}, Middle={attrs.get('middle_name')}, Last={attrs.get('last_name')}, Nickname={nickname}, Suffix={name_suffix}, PrayerGroup={prayer_group_val}")
        logging.info(f"Calculated DisplayName for {attrs.get('first_name')} {attrs.get('last_name')}: '{display_name}' (Format: '{display_fmt}')")
        
        # Construct QB payload
        qb_data = {
            "Title": name_prefix if name_prefix else "",
            "GivenName": attrs.get('first_name') or "",
            "MiddleName": (attrs.get('middle_name') or "")[:30],
            "FamilyName": attrs.get('last_name') or "",
            "Suffix": (nickname or "")[:10], # User requested nickname in Suffix
            "DisplayName": display_name[:500],
            "PrintOnCheckName": display_name[:110], # QB has 110 char limit for this
            "PrimaryEmailAddr": {"Address": primary_email if primary_email else None},
            "PrimaryPhone": {"FreeFormNumber": primary_phone if primary_phone else None},
            "Fax": {"FreeFormNumber": person_data['id']},  # Store PC ID in Fax field
            "BillAddr": {
                "Line1": addr.get('street_line_1'),
                "Line2": addr.get('street_line_2'),
                "City": addr.get('city'),
                "CountrySubDivisionCode": addr.get('state'),
                "PostalCode": addr.get('zip')
            },
            "CustomField": []
        }

        # Nickname
        nickname = attrs.get('nickname')
        if nickname:
            field_name = self.qb.custom_fields_map.get('nickname', 'Nickname')
            cf_entry = {
                "Name": field_name,
                "Type": "StringType",
                "StringValue": nickname
            }
            def_id = self.qb.discovered_definitions.get(field_name) or self.qb.custom_fields_fallback.get(field_name)
            if def_id:
                cf_entry["DefinitionId"] = def_id
            qb_data["CustomField"].append(cf_entry)

        # ID Number (Custom Field)
        field_name_id = self.qb.custom_fields_map.get('pc_id', 'ID number')
        cf_id_entry = {
            "Name": field_name_id,
            "Type": "StringType",
            "StringValue": str(person_data['id'])
        }
        def_id = self.qb.discovered_definitions.get(field_name_id) or self.qb.custom_fields_fallback.get(field_name_id)
        if def_id:
            cf_id_entry["DefinitionId"] = def_id
        qb_data["CustomField"].append(cf_id_entry)

        # Extract Prayer Group from PCO field data
        pco_prayer_group_id = self.pc.field_definitions.get('prayer group')
        prayer_group_val = None
        
        if pco_prayer_group_id:
            field_data_items = [item for item in included if item.get('type') == 'FieldDatum']
            for fd in field_data_items:
                ref_def_id = fd.get('relationships', {}).get('field_definition', {}).get('data', {}).get('id')
                if ref_def_id == pco_prayer_group_id:
                    prayer_group_val = fd.get('attributes', {}).get('value')
                    break
                    
        if prayer_group_val:
            field_name_pg = self.qb.custom_fields_map.get('prayer_group', 'PrayerGroup')
            cf_pg_entry = {
                "Name": field_name_pg,
                "Type": "StringType",
                "StringValue": str(prayer_group_val)[:50]
            }
            def_id = self.qb.discovered_definitions.get(field_name_pg) or self.qb.custom_fields_fallback.get(field_name_pg)
            if def_id:
                cf_pg_entry["DefinitionId"] = def_id
            qb_data["CustomField"].append(cf_pg_entry)
            # Also keep CompanyName for backward compatibility if needed, but the custom field is preferred now
            qb_data['CompanyName'] = str(prayer_group_val)[:50]

        return qb_data

    def _has_customer_changed(self, existing_qb, qb_payload):
        """
        Compare existing QB customer with payload and return a list of changes.
        Each change is a dict: {"field": str, "old": str, "new": str}
        """
        changes = []
        
        # Combine all fields to check for changes
        fields_to_check = [
            ("Title", "Title", "prefix"),
            ("GivenName", "GivenName", "first_name"),
            ("MiddleName", "MiddleName", "middle_name"),
            ("FamilyName", "FamilyName", "last_name"),
            ("Suffix", "Suffix", "suffix"),
            ("CompanyName", "CompanyName", "company_name"),
            ("DisplayName", "DisplayName", "display_name"),
            ("PrimaryEmailAddr", "Email", "email"),
            ("PrimaryPhone", "Phone", "phone"),
            ("BillAddr", "Address", "address")
        ]
        
        # Special check for Email, Phone, Address as they are nested
        for qb_key, display_name, internal_key in fields_to_check:
            qb_val = existing_qb.get(qb_key)
            new_val = qb_payload.get(qb_key)
            
            # Normalize for comparison
            if qb_key == "PrimaryEmailAddr":
                qb_val = qb_val.get('Address') if qb_val else ""
                new_val = new_val.get('Address') if new_val else ""
            elif qb_key == "PrimaryPhone":
                qb_val = qb_val.get('FreeFormNumber') if qb_val else ""
                new_val = new_val.get('FreeFormNumber') if new_val else ""
            elif qb_key == "BillAddr":
                # Compare critical address fields
                qb_val = f"{qb_val.get('Line1','')}, {qb_val.get('City','')}, {qb_val.get('CountrySubDivisionCode','')}, {qb_val.get('PostalCode','')}" if qb_val else ""
                new_val = f"{new_val.get('Line1','')}, {new_val.get('City','')}, {new_val.get('CountrySubDivisionCode','')}, {new_val.get('PostalCode','')}" if new_val else ""
            
            qb_val_str = str(qb_val or "").strip()
            new_val_str = str(new_val or "").strip()
            
            if qb_val_str != new_val_str:
                logging.info(f"Field {display_name} changed: '{qb_val_str}' -> '{new_val_str}'")
                changes.append({
                    "field": display_name,
                    "old": qb_val_str,
                    "new": new_val_str
                })
            else:
                logging.debug(f"Field {display_name} identical: '{qb_val_str}'")
        
        # Custom Fields (specifically Nickname, External ID, PrayerGroup)
        qb_custom_list = existing_qb.get('CustomField')
        if qb_custom_list is None:
            # If still None, we assume no custom fields exist on this object
            qb_custom_list = []
            
        n_custom = {cf['Name']: cf.get('StringValue', '') for cf in qb_payload.get('CustomField', [])}
        o_custom = {cf['Name']: cf.get('StringValue', '') for cf in qb_custom_list}
        
        logging.debug(f"Comparing Custom Fields for {existing_qb.get('DisplayName')}:")
        logging.debug(f"  Existing (o_custom): {json.dumps(o_custom)}")
        logging.debug(f"  New (n_custom): {json.dumps(n_custom)}")
        
        for name, nv in n_custom.items():
            # CRITICAL: Only compare/sync custom fields that were actually DISCOVERED in QuickBooks.
            # If a field is not discovered, it means we don't have a valid DefinitionId for it.
            # Comparing and sending undiscovered fields often results in "silent failures" in QB 
            # where the value is not persisted, causing an infinite loop of "changes" in our sync.
            if name not in self.qb.active_custom_field_names:
                logging.debug(f"Skipping comparison for custom field '{name}' as it was not discovered in QuickBooks settings.")
                continue

            ov = o_custom.get(name, "")
            if nv != ov:
                logging.info(f"Custom Field '{name}' changed: '{ov}' -> '{nv}'")
                changes.append({"field": name, "old": ov, "new": nv})
                
        return len(changes) > 0, changes

    def run(self):
        logging.info("Starting Sync Routine")
        self.summary['start_time'] = datetime.now().isoformat()
        self.summary['status'] = 'Running'
        self._save_summary_json()
        try:
            # 0. Fetch PCO and QB Custom Field Definitions
            self.pc.get_field_definitions()
            self.qb.get_custom_field_definitions()

            # Warn about missing custom fields once per run
            # Warn about missing custom fields once per run
            configured_custom_fields = set(self.qb.custom_fields_map.values())
            # For validation, we are a bit more permissive: if the field name is found at all (active or not), we'll try to sync it.
            # If it's totally missing from discovery, we'll really warn.
            missing_in_qb = [f for f in configured_custom_fields if f not in self.qb.active_custom_field_names and f != "External ID"] # External ID is often Fax fallback
            
            if missing_in_qb:
                msg = f"WARNING: The following configured custom fields were NOT discovered in QuickBooks: {', '.join(missing_in_qb)}. These will be skipped during sync to prevent redundant updates. Please ensure they are enabled for 'Customers' in QuickBooks Settings."
                logging.warning(msg)
                self.summary['logs'].append(msg)

            # 1. Fetch all PC members
            pc_person_ids = self.pc.get_list_results()
            logging.info(f"Found {len(pc_person_ids)} member IDs in PCO list {self.pco.head_of_household_list_id}")

            # 2. Fetch all QB customers and build lookup maps
            logging.info("Building lookup maps from QuickBooks customers...")
            all_qb_customers = self.qb.get_all_customers()
            if all_qb_customers:
                logging.debug(f"Sample Customer CustomFields: {json.dumps(all_qb_customers[0].get('CustomField'), indent=2)}")

            qb_id_map = {}
            qb_name_map = {}
            
            for qb_cust in all_qb_customers:
                pc_id = qb_cust.get('Fax', {}).get('FreeFormNumber', '').strip()
                if pc_id:
                    if pc_id in qb_id_map:
                        prev_id = qb_id_map[pc_id].get('Id')
                        curr_id = qb_cust.get('Id')
                        logging.warning(f"DUPLICATE PC_ID DETECTED in QuickBooks: PC ID {pc_id} is used by both QB ID {prev_id} and QB ID {curr_id}. Will use ID {curr_id} for sync.")
                    qb_id_map[pc_id] = qb_cust
                
                display_name = qb_cust.get('DisplayName')
                if display_name:
                    qb_name_map[display_name] = qb_cust
                    
            logging.info(f"Loaded {len(all_qb_customers)} customers from QB. Mapped {len(qb_id_map)} unique PC IDs.")

            processed_count = 0
            for pc_id in pc_person_ids:
                processed_count += 1
                
                # Save status every 10 records for dashboard progress
                if processed_count % 10 == 0:
                    self._save_summary_json()

                try:
                    # Fetch detailed data for mapping
                    detailed_pc = self.pc.get_person_details(pc_id)
                    person_name = detailed_pc['data']['attributes']['name']
                    
                    qb_payload = self._map_pc_to_qb(detailed_pc)

                    # Priority 1: Match by linked PC ID (Primary Unique Key)
                    existing_qb = qb_id_map.get(pc_id)
                    if existing_qb:
                        logging.debug(f"Match found by PC ID for {person_name} (ID: {existing_qb.get('Id')})")
                    
                    # Priority 2: Match by target DisplayName (to link existing unlinked records)
                    if not existing_qb:
                        target_display_name = qb_payload.get('DisplayName')
                        name_match = qb_name_map.get(target_display_name)
                        if name_match:
                            # NO-STEAL CHECK: Only match by name if the record has NO pc_id or the MINE pc_id
                            existing_pc_id = name_match.get('Fax', {}).get('FreeFormNumber', '').strip()
                            if not existing_pc_id or existing_pc_id == pc_id:
                                existing_qb = name_match
                                logging.info(f"Match found by target DisplayName for {target_display_name} (ID: {existing_qb.get('Id')}). Linking to PC ID {pc_id}.")
                            else:
                                logging.debug(f"Skipping name match for {target_display_name} (ID: {name_match.get('Id')}) because it belongs to a different PC ID ({existing_pc_id}).")

                    # Priority 3: Final fallback to raw PCO name (legacy cleanup)
                    if not existing_qb:
                        name_match = qb_name_map.get(person_name)
                        if name_match:
                            # NO-STEAL CHECK
                            existing_pc_id = name_match.get('Fax', {}).get('FreeFormNumber', '').strip()
                            if not existing_pc_id or existing_pc_id == pc_id:
                                existing_qb = name_match
                                logging.info(f"Match found by raw name for {person_name} (ID: {existing_qb.get('Id')}). Linking to PC ID {pc_id}.")
                            else:
                                logging.debug(f"Skipping raw name match for {person_name} because it belongs to a different PC ID.")

                    if existing_qb:
                        # Check for Name Conflict before updating
                        target_display_name = qb_payload.get('DisplayName')
                        if target_display_name and existing_qb.get('DisplayName') != target_display_name:
                            conflicting_qb = qb_name_map.get(target_display_name)
                            if conflicting_qb and conflicting_qb.get('Id') != existing_qb.get('Id'):
                                logging.warning(f"NAME CONFLICT: ID {existing_qb.get('Id')} ({person_name}) cannot be renamed to '{target_display_name}' because ID {conflicting_qb.get('Id')} already has that name. Please merge these records in QuickBooks.")
                                # We skip the name change in the payload to avoid 400 error, but keep other updates
                                qb_payload['DisplayName'] = existing_qb.get('DisplayName')

                    if existing_qb:
                        # IMPORTANT: Some environments don't return CustomField in bulk queries.
                        # If CustomField is missing in the bulk data, we fetch the full object to be 100% sure.
                        if existing_qb.get('CustomField') is None:
                            logging.debug(f"CustomField missing in bulk data for {person_name}, fetching detail...")
                            try:
                                existing_qb = self.qb.get_customer(existing_qb['Id'])
                                logging.debug(f"FULL QB CUSTOMER for {person_name}: {json.dumps(existing_qb, indent=2)}")
                            except Exception as e:
                                logging.warning(f"Could not fetch detailed QB customer for {person_name}: {e}")
                                # Continue with the bulk data if detail fetch fails

                        has_changed, changes = self._has_customer_changed(existing_qb, qb_payload)
                        if has_changed:
                            # Merge DefinitionIds from existing customer into payload
                            o_custom = {cf['Name']: cf.get('DefinitionId') for cf in existing_qb.get('CustomField', [])}
                            for cf in qb_payload.get('CustomField', []):
                                if cf['Name'] in o_custom:
                                    cf['DefinitionId'] = o_custom[cf['Name']]
                            
                            change_summary = ", ".join([c['field'] for c in changes])
                            logging.info(f"Info changed for {person_name}: {change_summary}")
                            for c in changes:
                                logging.info(f"  - {c['field']}: '{c['old']}' -> '{c['new']}'")
                            
                            logging.info(f"Updating QuickBooks for {person_name}...")
                            try:
                                updated_qb = self.qb.update_customer(existing_qb['Id'], existing_qb['SyncToken'], qb_payload)
                            except requests.HTTPError as e:
                                # 5010 is QuickBooks code for Stale Object
                                if e.response.status_code == 400 and "5010" in e.response.text:
                                    logging.warning(f"Stale Object Error for {person_name}. Re-fetching latest record and retrying...")
                                    existing_qb = self.qb.get_customer(existing_qb['Id'])
                                    updated_qb = self.qb.update_customer(existing_qb['Id'], existing_qb['SyncToken'], qb_payload)
                                else:
                                    logging.error(f"QB Update failed for {person_name}: {e.response.text}")
                                    raise
                            
                            # Update local maps with new data (important for SyncToken and PC_ID link)
                            qb_id_map[pc_id] = updated_qb
                            qb_name_map[person_name] = updated_qb
                            self._log_record("UPDATED", person_name, change_summary)
                            self._record_member_event(pc_id, person_name, 'UPDATED', change_summary, changes, display_name=qb_payload.get('DisplayName'))
                            self.summary['updated'] += 1
                        else:
                            logging.info(f"Skipping {person_name}: Info was not changed.")
                            self._record_member_event(pc_id, person_name, 'NO_CHANGE', 'Info was not changed', display_name=qb_payload.get('DisplayName'))
                    else:
                        new_qb = self.qb.create_customer(qb_payload)
                        # Link newly created customer in our maps
                        qb_id_map[pc_id] = new_qb
                        qb_name_map[person_name] = new_qb
                        self._log_record("CREATED", person_name)
                        self._record_member_event(pc_id, person_name, 'CREATED', display_name=qb_payload.get('DisplayName'))
                        self.summary['created'] += 1

                except Exception as e:
                    logging.error(f"Error syncing person ID {pc_id}: {e}")
                    self.summary['errors'] += 1
                    self._log_record("ERROR", f"ID {pc_id}", str(e))
                    self._record_member_event(pc_id, f"ID {pc_id}", 'ERROR', str(e))

                # Periodic status save for dashboard progress
                if processed_count % 10 == 0:
                    self._save_summary_json()

            self._save_member_history()
            self.send_summary_email()
            logging.info("Sync Routine completed successfully")
            
            # Update summary
            self.summary['status'] = 'Success'
            self.summary['end_time'] = datetime.now().isoformat()
            self._save_summary_json()

        except Exception as e:
            logging.error(f"Error during Sync Routine: {str(e)}")
            self.summary['status'] = 'Error'
            self.summary['fatal_error'] = str(e)
            self._save_summary_json()
            # Still try to send email if configured
            try:
                self.send_summary_email()
            except Exception as email_err:
                logging.error(f"Failed to send error notification email: {email_err}")
            self.summary['end_time'] = datetime.now().isoformat()
            self._log_record("FATAL ERROR", "", str(e))
            self._save_summary_json()
            self.send_summary_email(fatal_error=str(e))

    def send_summary_email(self, fatal_error: str = None):
        email_cfg = self.config.get('email', {})
        sender_email = os.getenv('SMTP_SENDER_EMAIL') or email_cfg.get('sender_email')
        sender_password = os.getenv('SMTP_PASSWORD')
        recipient_email = os.getenv('SMTP_RECIPIENT_EMAIL') or email_cfg.get('recipient_email', 'admin@example.com')
        
        if not sender_email or not sender_password:
            logging.warning("Email credentials missing in .env. Skipping summary email.")
            return
            
        if recipient_email == 'admin@example.com':
            logging.warning("Recipient email is set to the default placeholder. Add SMTP_RECIPIENT_EMAIL to .env to receive summaries. Skipping email.")
            return

        subject = f"PC to QB Sync Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body = f"Sync Summary:\n"
        if fatal_error:
            body += f"FATAL ERROR: {fatal_error}\n\n"
        
        body += f"Created: {self.summary['created']}\n"
        body += f"Updated: {self.summary['updated']}\n"
        body += f"Errors: {self.summary['errors']}\n\n"
        body += "Logs:\n" + "\n".join(self.summary['logs'])

        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = recipient_email

        smtp_server = os.getenv('SMTP_SERVER') or email_cfg.get('smtp_server', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT') or email_cfg.get('smtp_port', 587))
        
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            logging.info("Summary email sent")
        except Exception as e:
            logging.error(f"Failed to send summary email: {e}")

if __name__ == "__main__":
    try:
        config = load_config()
        log_file = setup_logging(config)
        rotate_logs(keep=10)
        logging.info(f"Logging to: {log_file}")
        routine = SyncRoutine(config)
        routine.run()
    except FileNotFoundError:
        print("config.json not found. Please create it from config.json.template")
    except Exception as e:
        print(f"Error: {e}")
