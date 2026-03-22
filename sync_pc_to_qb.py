import os
import json
import logging
import requests
import smtplib
import glob
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv, set_key

# Load environment variables from .env file
load_dotenv()

# Load configuration
def load_config(config_path: str = 'config.json') -> Dict[str, Any]:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(config_path, 'r') as f:
        return json.load(f)

# Logs helper
LOG_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logging(config: Dict[str, Any]):
    log_cfg = config.get('logging', {})
    
    # Generate timestamped filename inside logs/ directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"sync_{timestamp}.log")
    
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

def rotate_logs(keep: int = 10):
    """Keep only the 'keep' most recent sync log files."""
    log_files = sorted(glob.glob(os.path.join(LOG_DIR, "sync_*.log")), key=os.path.getmtime, reverse=True)
    if len(log_files) > keep:
        logs_to_delete = log_files[keep:]
        for log_file in logs_to_delete:
            try:
                os.remove(log_file)
            except Exception:
                pass

class PlanningCenterClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config['base_url']
        self.head_of_household_list_id = config['head_of_household_list_id']
        # Load secrets from environment
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
        params = {'include': 'emails,addresses,phone_numbers,field_data'}
        response = requests.get(url, auth=self.auth, params=params)
        response.raise_for_status()
        return response.json()

class QuickBooksClient:
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config['base_url']
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
            env_path = os.path.join(os.path.dirname(__file__), '.env')
            if os.path.exists(env_path):
                set_key(env_path, 'QB_REFRESH_TOKEN', self.refresh_token)
                logging.info("New QB refresh token automatically saved to .env")
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
        """Fetch custom field definitions from Preferences API."""
        url = f"{self.base_url}/v3/company/{self.realm_id}/preferences"
        # minorversion 70 + include=enhancedAllCustomFields is recommended for Advanced custom fields
        params = {'minorversion': self.minorversion, 'include': 'enhancedAllCustomFields'}
        response = requests.get(url, headers=self._get_headers(), params=params)
        response.raise_for_status()
        
        data = response.json()
        logging.debug(f"Raw QB Preferences: {json.dumps(data, indent=2)}")
        
        prefs = data.get('Preferences', {})
        sales_prefs = prefs.get('SalesFormsPrefs', {})
        custom_fields = sales_prefs.get('CustomField', [])
        
        # Flatten nested CustomField structure
        flat_fields = []
        for cf in custom_fields:
            if isinstance(cf.get('CustomField'), list):
                flat_fields.extend(cf['CustomField'])
            else:
                flat_fields.append(cf)

        for cf in flat_fields:
            name = cf.get('Name')
            def_id = cf.get('DefinitionId')
            
            # For QBO Plus, the 'Name' is the label you see in the UI.
            # However, if not fully enabled, it might show 'SalesFormsPrefs.UseSalesCustom1' etc.
            if name and def_id:
                self.discovered_definitions[name] = def_id
                
        # If discovery found nothing but we are on Plus, we can't assume much 
        # except that the user needs to enable them. 
        logging.info(f"Discovered QB Custom Fields: {self.discovered_definitions}")

    def get_all_customers(self) -> List[Dict[str, Any]]:
        """Fetch all customers from QB to build a local lookup map."""
        customers = []
        start_position = 1
        max_results = 100
        
        while True:
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
        """Fetch a single customer by ID."""
        url = f"{self.base_url}/v3/company/{self.realm_id}/customer/{customer_id}"
        params = {'minorversion': self.minorversion}
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
        response = requests.post(url, headers=self._get_headers(), json=payload, params=params)
        if not response.ok:
            # We raise here, the caller handles Stale Object (5010) specifically
            response.raise_for_status()
        return response.json().get('Customer', {})

class SyncRoutine:
    def __init__(self, config: Dict[str, Any]):
        # Dynamically reload environment variables on every start
        # so manual .env token updates take effect without restarting the app.
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(dotenv_path=env_path, override=True)

        self.config = config
        self.pc = PlanningCenterClient(config['planning_center'])
        self.qb = QuickBooksClient(config['quickbooks'])
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

    def _save_summary_json(self):
        """Save the latest summary to a local JSON file."""
        try:
            with open("latest_sync_status.json", "w") as f:
                json.dump(self.summary, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save status JSON: {e}")

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

        # Construct QB payload
        qb_data = {
            "GivenName": attrs.get('first_name'),
            "MiddleName": attrs.get('middle_name')[:30] if attrs.get('middle_name') else None,
            "FamilyName": attrs.get('last_name'),
            "DisplayName": attrs.get('name'),
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
            # Custom fields are now optional/best-effort
            "CustomField": []
        }

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
            # Map Prayer Group to QuickBooks CompanyName field (max length 50)
            qb_data['CompanyName'] = str(prayer_group_val)[:50]

        return qb_data

    def _has_customer_changed(self, existing_qb, qb_payload):
        for field in ['GivenName', 'MiddleName', 'FamilyName', 'DisplayName', 'CompanyName']:
            if (qb_payload.get(field) or "") != (existing_qb.get(field) or ""):
                return True, f"{field} changed"
                
        n_email = qb_payload.get('PrimaryEmailAddr', {}).get('Address') if qb_payload.get('PrimaryEmailAddr') else ""
        o_email = existing_qb.get('PrimaryEmailAddr', {}).get('Address') if existing_qb.get('PrimaryEmailAddr') else ""
        if n_email != o_email: return True, "Email changed"

        n_phone = qb_payload.get('PrimaryPhone', {}).get('FreeFormNumber') if qb_payload.get('PrimaryPhone') else ""
        o_phone = existing_qb.get('PrimaryPhone', {}).get('FreeFormNumber') if existing_qb.get('PrimaryPhone') else ""
        if n_phone != o_phone: return True, "Phone changed"

        n_addr = qb_payload.get('BillAddr', {})
        o_addr = existing_qb.get('BillAddr', {})
        if n_addr or o_addr:
            for field in ['Line1', 'Line2', 'City', 'CountrySubDivisionCode', 'PostalCode']:
                if (n_addr.get(field) or "") != (o_addr.get(field) or ""):
                    return True, f"Address {field} changed"
                
        return False, "No changes detected"

    def run(self):
        logging.info("Starting Sync Routine")
        self.summary['start_time'] = datetime.now().isoformat()
        self.summary['status'] = 'Running'
        self._save_summary_json()
        try:
            # 0. Fetch PCO Custom Field Definitions
            self.pc.get_field_definitions()

            # 1. Fetch all PC members
            pc_person_ids = self.pc.get_list_results()
            logging.info(f"Found {len(pc_person_ids)} member IDs in PC Head of Household list")

            # 2. Fetch all QB customers and build lookup maps
            logging.info("Building lookup maps from QuickBooks customers...")
            all_qb_customers = self.qb.get_all_customers()
            if all_qb_customers:
                logging.debug(f"Sample Customer CustomFields: {json.dumps(all_qb_customers[0].get('CustomField'), indent=2)}")

            qb_id_map = {}
            qb_name_map = {}
            
            for qb_cust in all_qb_customers:
                pc_id = self._get_pc_id_from_qb_customer(qb_cust)
                if pc_id:
                    qb_id_map[pc_id] = qb_cust
                
                display_name = qb_cust.get('DisplayName')
                if display_name:
                    qb_name_map[display_name] = qb_cust
                    
            logging.info(f"Mapped {len(qb_id_map)} customers by PC_ID and {len(qb_name_map)} by Name from QB")

            # 3. Sync
            for pc_id in pc_person_ids:
                try:
                    # Fetch detailed data for mapping
                    detailed_pc = self.pc.get_person_details(pc_id)
                    person_name = detailed_pc['data']['attributes']['name']
                    
                    qb_payload = self._map_pc_to_qb(detailed_pc)

                    # Priority 1: Match by linked PC ID
                    existing_qb = qb_id_map.get(pc_id)
                    
                    # Priority 2: Fallback to match by Name (to prevent duplicates and link existing ones)
                    if not existing_qb:
                        existing_qb = qb_name_map.get(person_name)
                        if existing_qb:
                            logging.info(f"Match found by name for {person_name}. Linking to PC ID {pc_id}.")

                    if existing_qb:
                        has_changed, change_reason = self._has_customer_changed(existing_qb, qb_payload)
                        if has_changed:
                            logging.info(f"Info changed for {person_name}: {change_reason}. Updating QuickBooks...")
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
                            self._log_record("UPDATED", person_name)
                            self.summary['updated'] += 1
                        else:
                            logging.info(f"Skipping {person_name}: Info was not changed.")
                    else:
                        new_qb = self.qb.create_customer(qb_payload)
                        # Link newly created customer in our maps
                        qb_id_map[pc_id] = new_qb
                        qb_name_map[person_name] = new_qb
                        self._log_record("CREATED", person_name)
                        self.summary['created'] += 1

                except Exception as e:
                    logging.error(f"Error syncing person ID {pc_id}: {e}")
                    self.summary['errors'] += 1
                    self._log_record("ERROR", f"ID {pc_id}", str(e))

            self.send_summary_email()
            logging.info("Sync Routine completed successfully")
            
            # Update summary
            self.summary['status'] = 'Success'
            self.summary['end_time'] = datetime.now().isoformat()
            self._save_summary_json()

        except Exception as e:
            logging.critical(f"Sync Routine failed: {e}")
            self.summary['status'] = 'Failed'
            self.summary['errors'] += 1
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
