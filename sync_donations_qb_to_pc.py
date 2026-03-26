"""
Donation Reverse Sync: QuickBooks → Planning Center Giving
Fetches Sales Receipts (or Payments) from QB and creates Donations in PCO Giving.
"""
import os
import json
import logging
import time
import requests
import smtplib
import glob
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file (priority: config/.env)
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
ENV_PATH = os.path.join(BASE_DIR, 'config', '.env')
if not os.path.isfile(ENV_PATH):
    fallback_path = os.path.join(BASE_DIR, '.env')
    if os.path.isfile(fallback_path):
        ENV_PATH = fallback_path

load_dotenv(dotenv_path=ENV_PATH, override=True)

# ---------------------------------------------------------------------------
# Planning Center Giving API Client
# ---------------------------------------------------------------------------
class PlanningCenterGivingClient:
    """Wraps Planning Center Giving API v2."""

    BASE_URL = "https://api.planningcenteronline.com/giving/v2"

    def __init__(self):
        self.app_id = os.getenv('PCO_APP_ID')
        self.token = os.getenv('PCO_PAT')
        if not self.app_id or not self.token:
            raise ValueError("PCO_APP_ID or PCO_PAT missing in .env")
        self.auth = (self.app_id, self.token)

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Content-Type": "application/json",
            "X-PCO-API-Version": "2025-01-01"
        }
        logging.debug(f"PCO Giving GET {url}")
        resp = requests.get(url, auth=self.auth, headers=headers, params=params)
        if not resp.ok:
            logging.error(f"PCO Giving GET failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict) -> dict:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Content-Type": "application/json",
            "X-PCO-API-Version": "2025-01-01"
        }
        logging.debug(f"PCO Giving POST {url}")
        resp = requests.post(url, auth=self.auth, headers=headers, json=payload)
        if not resp.ok:
            logging.error(f"PCO Giving POST failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, payload: dict) -> dict:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Content-Type": "application/json",
            "X-PCO-API-Version": "2025-01-01"
        }
        logging.debug(f"PCO Giving PATCH {url}")
        resp = requests.patch(url, auth=self.auth, headers=headers, json=payload)
        if not resp.ok:
            logging.error(f"PCO Giving PATCH failed: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
        return resp.json()

    # -- Funds ---------------------------------------------------------------
    def get_funds(self) -> Dict[str, str]:
        """Fetch all funds. Returns {fund_name: fund_id} mapped by original name."""
        funds = {}
        for item in self.get_all_funds():
            funds[item["name"]] = item["id"]
        return funds

    def get_all_funds(self) -> List[Dict[str, str]]:
        """Fetch all funds. Returns list of {"id": X, "name": Y}."""
        funds = []
        path = "/funds"
        while path:
            data = self._get(path)
            for item in data.get("data", []):
                name = item["attributes"].get("name", "")
                funds.append({
                    "id": item["id"],
                    "name": name
                })
            next_link = data.get("links", {}).get("next")
            if next_link:
                path = next_link.replace(self.BASE_URL, "")
            else:
                path = None
        logging.info(f"Loaded {len(funds)} PC Giving funds.")
        return funds

    # -- People / Donors ----------------------------------------------------
    def find_person_by_id(self, pc_person_id: str) -> Optional[str]:
        """
        Check if a person exists in Giving.
        PCO Giving has its own 'people' resource linked to PCO People IDs.
        Returns the Giving person id if found, else None.
        """
        try:
            # The Giving API links people by their PCO People ID
            data = self._get(f"/people", params={"where[id]": pc_person_id})
            results = data.get("data", [])
            if results:
                return results[0]["id"]
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise
        return None

    # -- Batches -------------------------------------------------------------
    def create_batch(self, description: str) -> str:
        """Create a new batch. Returns batch ID."""
        payload = {
            "data": {
                "type": "Batch",
                "attributes": {
                    "description": description
                }
            }
        }
        data = self._post("/batches", payload)
        batch_id = data["data"]["id"]
        logging.info(f"Created PC Giving batch {batch_id}: {description}")
        return batch_id

    def commit_batch(self, batch_id: str):
        """Commit (finalize) a batch."""
        # PCO Giving v2 requires a POST to the /commit action endpoint
        try:
            self._post(f"/batches/{batch_id}/commit", {})
            logging.info(f"Committed PC Giving batch {batch_id}")
        except requests.HTTPError as e:
            logging.warning(f"Could not commit batch {batch_id}: {e}")

    # -- Donations -----------------------------------------------------------
    def create_donation(
        self,
        batch_id: str,
        person_id: Optional[str],
        received_at: str,
        designations: List[Dict[str, Any]],
        payment_method: str = "cash",
        payment_source_id: Optional[str] = None,
        memo: Optional[str] = None
    ) -> str:
        """Create a donation with one or more designations within a batch."""
        donation_attrs = {
            "payment_method": payment_method,
            "received_at": received_at
        }
        if memo:
            donation_attrs["memo"] = memo

        donation_relationships = {}
        if person_id:
            donation_relationships["person"] = {
                "data": {
                    "type": "Person",
                    "id": person_id
                }
            }
        
        if payment_source_id:
            donation_relationships["payment_source"] = {
                "data": {
                    "type": "PaymentSource",
                    "id": payment_source_id
                }
            }

        payload = {
            "data": {
                "type": "Donation",
                "attributes": donation_attrs,
                "relationships": donation_relationships
            },
            "included": designations
        }

        try:
            data = self._post(f"/batches/{batch_id}/donations", payload)
            donation_id = data["data"]["id"]
            logging.debug(f"Created donation {donation_id} in batch {batch_id}")
            return donation_id
        except requests.HTTPError as e:
            error_body = ""
            if e.response is not None:
                error_body = e.response.text
            
            if memo and "memo cannot be assigned" in error_body:
                logging.warning(f"memo attribute rejected by API. Retrying without memo...")
                del payload["data"]["attributes"]["memo"]
                data = self._post(f"/batches/{batch_id}/donations", payload)
                donation_id = data["data"]["id"]
                
                # Fallback: Create an internal Note instead
                try:
                    logging.info(f"Creating internal note for donation {donation_id} as fallback for memo")
                    note_payload = {
                        "data": {
                            "type": "Note",
                            "attributes": {
                                "body": f"QB Product/Service: {memo}"
                            }
                        }
                    }
                    self._post(f"/donations/{donation_id}/note", note_payload)
                except Exception as note_err:
                    logging.warning(f"Could not create fallback note: {note_err}")
                
                return donation_id
            raise


# ---------------------------------------------------------------------------
# Donation Sync Routine
# ---------------------------------------------------------------------------
class DonationSyncRoutine:
    """Orchestrates the reverse sync from QuickBooks → Planning Center Giving."""

    def __init__(self, config: Dict[str, Any], donation_settings: Optional[Dict[str, Any]] = None):
        load_dotenv(override=True)

        self.config = config
        self.donation_config = config.get("donation_sync", {})
        self.base_dir = os.path.dirname(os.path.realpath(__file__))

        # State and History persistence
        self.state_path = os.path.join(self.base_dir, "data", "donation_sync_state.json")
        self.history_path = os.path.join(self.base_dir, "data", "donation_sync_history.json")
        self.state = self._load_state()
        self.donation_history = self._load_history()

        # Settings (can be overridden from web portal via donation_sync_settings.json)
        self.settings_path = os.path.join(self.base_dir, "data", "donation_sync_settings.json")
        self.settings = donation_settings if donation_settings is not None else self._load_settings()

        # Clients
        self.pco = PlanningCenterGivingClient()
        self.qb = self._init_qb_client()

        # Summary for logging/email
        self.summary = {
            "status": "Running",
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "duration_seconds": 0,
            "donations_created": 0,
            "donations_skipped": 0,
            "errors": 0,
            "logs": []
        }

    def _init_qb_client(self):
        """Reuse the existing QuickBooksClient from sync_pc_to_qb."""
        from sync_pc_to_qb import QuickBooksClient
        return QuickBooksClient(self.config.get("quickbooks", {}))

    def _load_state(self) -> Dict[str, Any]:
        """Load sync state from disk."""
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Could not load donation_sync_state.json: {e}")
        return {
            "last_sync_time": None,
            "synced_transaction_ids": [],
            "last_summary": {}
        }

    def _save_state(self, retries=5, delay=0.1):
        """Persist state to disk using atomic rename with retries."""
        for i in range(retries):
            try:
                temp_path = self.state_path + ".tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(self.state, f, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, self.state_path)
                return True
            except OSError as e:
                if e.errno == 35: # Resource deadlock avoided
                    if i < retries - 1:
                        time.sleep(delay)
                        continue
                logging.error(f"Failed to save donation_sync_state.json (attempt {i+1}): {e}")
                if i == retries - 1: raise
            except Exception as e:
                logging.error(f"Unexpected error saving donation sync state: {e}")
                break
        return False

    def _load_history(self) -> Dict[str, Any]:
        """Load persistent donation history (per-transaction)."""
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.error(f"Failed to load donation history: {e}")
        return {}

    def _save_history(self, retries=5, delay=0.1):
        """Save donation history to file using atomic rename with retries."""
        for i in range(retries):
            try:
                temp_path = self.history_path + ".tmp"
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.donation_history, f, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, self.history_path)
                return True
            except OSError as e:
                if e.errno == 35: # Resource deadlock avoided
                    if i < retries - 1:
                        time.sleep(delay)
                        continue
                logging.error(f"Failed to save donation history (attempt {i+1}): {e}")
                if i == retries - 1: raise
            except Exception as e:
                logging.error(f"Unexpected error saving donation history: {e}")
                break
        return False

    def _record_donation_event(self, qb_txn_id: str, donor_name: str, action: str, detail: str = "", pc_person_id: Optional[str] = None):
        """Record a sync event for a specific QB transaction."""
        if qb_txn_id not in self.donation_history:
            self.donation_history[qb_txn_id] = {
                'donor_name': donor_name,
                'events': []
            }
        self.donation_history[qb_txn_id]['donor_name'] = donor_name
        if pc_person_id:
            logging.info(f"Setting pco_id={pc_person_id} for txn {qb_txn_id}")
            self.donation_history[qb_txn_id]['pco_id'] = pc_person_id
        else:
            logging.warning(f"No pc_person_id provided for txn {qb_txn_id}")
            
        self.donation_history[qb_txn_id]['events'].append({
            'date': datetime.now().isoformat(),
            'action': action,
            'detail': detail
        })
        # Keep last 10 events per transaction
        self.donation_history[qb_txn_id]['events'] = self.donation_history[qb_txn_id]['events'][-10:]

    def _load_settings(self) -> Dict[str, Any]:
        """Load dynamic settings from web portal config."""
        defaults = {
            "transaction_type": "SalesReceipt",
            "lookback_days": 30,
            "default_fund_name": "General Fund",
            "fund_mapping": {},
            "payment_method_map": {
                "Cash": "cash",
                "Check": "check",
                "Credit Card": "credit_card",
                "ACH": "ach"
            }
        }
        if os.path.exists(self.settings_path):
            try:
                with open(self.settings_path, "r", encoding="utf-8") as f:
                    saved = json.load(f)
                defaults.update(saved)
            except Exception as e:
                logging.warning(f"Could not load donation_sync_settings.json: {e}")
        return defaults

    def _save_settings(self):
        """Persist settings to disk."""
        try:
            with open(self.settings_path, "w", encoding="utf-8") as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save donation_sync_settings.json: {e}")

    def _log_record(self, action: str, detail: str = ""):
        msg = f"[DonationSync] {action}: {detail}"
        logging.info(msg)
        self.summary["logs"].append(msg)

    # -- QuickBooks: Fetch Transactions ------------------------------------
    def _query_qb_transactions(self) -> List[Dict[str, Any]]:
        """Query QB for Sales Receipts or Payments since last sync."""
        txn_type = self.settings.get("transaction_type", "SalesReceipt")
        lookback_days = self.settings.get("lookback_days", 30)

        # Determine start date
        if self.state.get("last_sync_time"):
            since_date = self.state["last_sync_time"][:10]  # YYYY-MM-DD
        else:
            since_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        transactions = []
        start_position = 1
        max_results = 100

        while True:
            query = (
                f"SELECT * FROM {txn_type} "
                f"WHERE MetaData.CreateTime >= '{since_date}' "
                f"STARTPOSITION {start_position} MAXRESULTS {max_results}"
            )
            url = f"{self.qb.base_url}/v3/company/{self.qb.realm_id}/query"
            params = {"query": query, "minorversion": self.qb.minorversion}

            logging.info(f"Querying QB: {query}")
            resp = requests.get(url, headers=self.qb._get_headers(), params=params)

            if resp.status_code == 401:
                logging.info("QB token expired, refreshing...")
                self.qb._refresh_access_token()
                resp = requests.get(url, headers=self.qb._get_headers(), params=params)

            resp.raise_for_status()
            data = resp.json().get("QueryResponse", {})
            batch = data.get(txn_type, [])
            transactions.extend(batch)

            if len(batch) < max_results:
                break
            start_position += max_results

        logging.info(f"Found {len(transactions)} {txn_type} records from QB since {since_date}")
        return transactions

    # -- Mapping helpers ---------------------------------------------------
    def _build_qb_customer_pc_id_map(self) -> Dict[str, str]:
        """Build a map of QB Customer ID → PCO Person ID using existing QB customers.
        The PCO Person ID is stored in the Fax.FreeFormNumber field."""
        logging.info("Building QB Customer → PCO Person ID map...")
        all_customers = self.qb.get_all_customers()
        mapping = {}
        for cust in all_customers:
            qb_cust_id = cust.get("Id")
            fax = cust.get("Fax", {})
            pc_id = fax.get("FreeFormNumber")
            if qb_cust_id and pc_id:
                mapping[qb_cust_id] = pc_id
        logging.info(f"Mapped {len(mapping)} QB customers to PCO Person IDs")
        return mapping

    def _map_payment_method(self, qb_txn: Dict[str, Any]) -> str:
        """Map QB payment method to PCO payment method string."""
        pm_ref = qb_txn.get("PaymentMethodRef", {})
        pm_name = pm_ref.get("name", "")
        mapping = self.settings.get("payment_method_map", {})
        return mapping.get(pm_name, "cash")

    def _get_line_items_with_amounts(self, qb_txn: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract line items from a QB transaction, each with account name and amount."""
        items = []
        for line in qb_txn.get("Line", []):
            # CRITICAL: Only process SalesItemLineDetail to avoid duplicates from subtotal lines!
            if line.get("DetailType") != "SalesItemLineDetail":
                continue

            detail = line.get("SalesItemLineDetail") or {}
            amount = line.get("Amount", 0)
            if amount <= 0:
                continue

            # Try to get account/item name for fund mapping
            item_ref = detail.get("ItemRef", {})
            account_ref = line.get("AccountRef") or detail.get("AccountRef", {})
            item_name = item_ref.get("name", "")
            account_name = account_ref.get("name", "")

            items.append({
                "amount": amount,
                "item_name": item_name,
                "account_name": account_name
            })
        return items

    def _resolve_fund_id(self, item_name: str, account_name: str, fund_map: Dict[str, str]) -> Optional[str]:
        """Resolve a PCO fund ID from item/account name using the dynamic fund mapping."""
        # Priority 1: Check user-defined fund mapping (from web portal)
        user_mapping = self.settings.get("fund_mapping", {})

        for search_name in [item_name, account_name]:
            if not search_name:
                continue
            # Check exact match in user mapping (case insensitive)
            mapped_fund = user_mapping.get(search_name)
            if mapped_fund:
                # Try finding in fund_map (case insensitive)
                for f_name, f_id in fund_map.items():
                    if f_name.lower() == mapped_fund.lower():
                        return f_id

            # Check direct match in PC funds (case insensitive)
            for f_name, f_id in fund_map.items():
                if f_name.lower() == search_name.lower():
                    return f_id

        # Fallback to default fund (case insensitive)
        default_name = self.settings.get("default_fund_name", "General Fund")
        for f_name, f_id in fund_map.items():
            if f_name.lower() == default_name.lower():
                return f_id
        return None

    # -- Main sync ---------------------------------------------------------
    def run(self):
        """Execute the donation reverse sync."""
        lock_path = os.path.join(self.base_dir, "data", "donation_sync.lock")
        if not os.path.exists(lock_path):
             # Fallback
             lock_path = os.path.join(self.base_dir, "donation_sync.lock")
             
        if os.path.exists(lock_path):
            # Check if lock is old (e.g. > 1 hour)
            mtime = os.path.getmtime(lock_path)
            if (datetime.now().timestamp() - mtime) < 3600:
                logging.warning("Donation sync already in progress (lock file exists).")
                return
            else:
                logging.info("Old lock file found, removing...")
                os.remove(lock_path)

        with open(lock_path, "w") as f:
            f.write(str(os.getpid()))

        try:
            logging.info("=" * 60)
            logging.info("Starting Donation Reverse Sync (QB → PCO Giving)")
            logging.info("=" * 60)
            self.summary["start_time"] = datetime.now().isoformat()
            self.summary["status"] = "Running"
            self._save_summary_status()

            # 1. Fetch PCO funds
            fund_map = self.pco.get_funds()
            if not fund_map:
                raise ValueError("No funds found in Planning Center Giving. Please create at least one fund.")

            # 2. Build QB Customer → PCO Person mapping
            customer_pc_map = self._build_qb_customer_pc_id_map()

            # 3. Query QB for transactions
            transactions = self._query_qb_transactions()

            # 4. Filter out already-synced transactions
            synced_ids = set(self.state.get("synced_transaction_ids", []))
            new_transactions = [
                txn for txn in transactions
                if txn.get("Id") not in synced_ids
            ]
            logging.info(f"After filtering: {len(new_transactions)} new transactions to sync "
                         f"({len(transactions) - len(new_transactions)} already synced)")

            if not new_transactions:
                self._log_record("NO_NEW_DONATIONS", "No new transactions to sync.")
                # We still want to update 'Last Sync' and 'last_summary' in the state
                self.state["last_sync_time"] = datetime.now().isoformat()
                self.state["last_summary"] = {
                    "donations_created": 0,
                    "donations_skipped": 0,
                    "errors": 0,
                    "batch_id": None,
                    "timestamp": datetime.now().isoformat()
                }
                self._save_state()
                
                self.summary["status"] = "Success"
                self.summary["end_time"] = datetime.now().isoformat()
                self._save_summary_status()
                return

            # 5. Create a PC Giving batch
            batch_desc = f"QB Sync - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            batch_id = self.pco.create_batch(batch_desc)

            # 6. Process each transaction
            processed_count = 0
            for txn in new_transactions:
                processed_count += 1
                # Save status every 10 transactions for dashboard progress
                if processed_count % 10 == 0:
                    self._save_summary_status()
                    self._save_state()

                try:
                    txn_id = txn.get("Id", "unknown")
                    txn_type = self.settings.get("transaction_type", "SalesReceipt")

                    # Get customer → person mapping
                    customer_ref = txn.get("CustomerRef", {})
                    qb_customer_id = customer_ref.get("value")
                    customer_name = customer_ref.get("name", "Unknown")

                    pc_person_id = None
                    if qb_customer_id:
                        pc_person_id = customer_pc_map.get(qb_customer_id)

                    if not pc_person_id:
                        self._log_record(
                            "SKIPPED",
                            f"{txn_type} #{txn_id} ({customer_name}): No PC Person ID linked"
                        )
                        self._record_donation_event(txn_id, customer_name, 'SKIPPED', 'No PC Person ID linked', pc_person_id)
                        self.summary["donations_skipped"] += 1
                        
                        # Mark as synced even if skipped (missing link)
                        self.state["synced_transaction_ids"].append(txn_id)
                        self.state["synced_transaction_ids"] = self.state["synced_transaction_ids"][-10000:]
                        # self._save_state()  # Moved to periodic save
                        continue

                    # Get transaction date
                    txn_date = txn.get("TxnDate", datetime.now().strftime("%Y-%m-%d"))
                    received_at = f"{txn_date}T00:00:00Z"

                    # Payment method
                    payment_method = self._map_payment_method(txn)

                    # Get line items for fund allocation
                    line_items = self._get_line_items_with_amounts(txn)
                    
                    # Consolidate line items by fund
                    allocations = {} # fund_id -> total_cents
                    
                    if line_items:
                        for li in line_items:
                            fund_id = self._resolve_fund_id(
                                li["item_name"], li["account_name"], fund_map
                            ) or list(fund_map.values())[0]
                            
                            amount_cents = int(round(li["amount"] * 100))
                            allocations[fund_id] = allocations.get(fund_id, 0) + amount_cents
                    else:
                        # Fallback to total amount
                        total_amt = txn.get("TotalAmt", 0)
                        default_name = self.settings.get("default_fund_name", "General Fund")
                        fund_id = next((f_id for f_name, f_id in fund_map.items() if f_name.lower() == default_name.lower()), list(fund_map.values())[0])
                        allocations[fund_id] = int(round(total_amt * 100))

                    # Create Designations
                    designations = []
                    for f_id, cents in allocations.items():
                        if cents <= 0: continue
                        designations.append({
                            "type": "Designation",
                            "attributes": { "amount_cents": cents },
                            "relationships": {
                                "fund": { "data": { "type": "Fund", "id": f_id } }
                            }
                        })

                    if designations:
                        # Collect product/service names for the memo
                        item_names = sorted(list(set(li.get("item_name") for li in line_items if li.get("item_name"))))
                        memo = " | ".join(item_names) if item_names else None

                        payment_source_id = self.settings.get("payment_source_id", "58369")
                        self.pco.create_donation(
                            batch_id=batch_id,
                            person_id=pc_person_id,
                            received_at=received_at,
                            designations=designations,
                            payment_method=payment_method,
                            payment_source_id=payment_source_id,
                            memo=memo
                        )

                        self._log_record(
                            "CREATED",
                            f"{txn_type} #{txn_id} → {customer_name} (PC ID: {pc_person_id}), "
                            f"${txn.get('TotalAmt', 0):.2f} ({len(designations)} funds)"
                        )
                        self._record_donation_event(txn_id, customer_name, 'CREATED', f"Created in PCO Batch {batch_id}", pc_person_id)
                        self.summary["donations_created"] += 1
                        
                        # Incremental Persistence: Save synced ID immediately
                        self.state["synced_transaction_ids"].append(txn_id)
                        self.state["synced_transaction_ids"] = self.state["synced_transaction_ids"][-10000:]
                        # self._save_state()  # Moved to periodic save
                        self._save_summary_status()

                except Exception as e:
                    self._log_record("ERROR", f"Transaction {txn.get('Id', '?')}: {e}")
                    self._record_donation_event(txn.get("Id", "unknown"), customer_name, 'ERROR', str(e), pc_person_id)
                    self.summary["errors"] += 1
                    logging.error(f"Error processing transaction: {e}", exc_info=True)

            # 7. Commit the batch
            if self.summary["donations_created"] > 0:
                self.pco.commit_batch(batch_id)

            # 8. Update summary in state
            self.state["last_sync_time"] = datetime.now().isoformat()
            self.state["last_summary"] = {
                "donations_created": self.summary["donations_created"],
                "donations_skipped": self.summary["donations_skipped"],
                "errors": self.summary["errors"],
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat()
            }
            self._save_state()
            self._save_history()

            # 9. Finalize
            self.summary["status"] = "Success"
            self.summary["end_time"] = datetime.now().isoformat()
            
            # Calculate duration
            start_dt = datetime.fromisoformat(self.summary["start_time"])
            end_dt = datetime.fromisoformat(self.summary["end_time"])
            self.summary["duration_seconds"] = int((end_dt - start_dt).total_seconds())
            
            self._save_summary_status()
            self.send_summary_email()

            logging.info(f"Donation sync complete: {self.summary['donations_created']} created, "
                         f"{self.summary['donations_skipped']} skipped, "
                         f"{self.summary['errors']} errors")


        except Exception as e:
            logging.critical(f"Donation sync failed: {e}", exc_info=True)
            self.summary["status"] = "Failed"
            self.summary["errors"] += 1
            self.summary["end_time"] = datetime.now().isoformat()
            
            # Calculate duration even on failure
            try:
                start_dt = datetime.fromisoformat(self.summary["start_time"])
                end_dt = datetime.fromisoformat(self.summary["end_time"])
                self.summary["duration_seconds"] = int((end_dt - start_dt).total_seconds())
            except Exception:
                pass
                
            self._log_record("FATAL ERROR", str(e))
            self._save_summary_status()
            self.send_summary_email(fatal_error=str(e))
        finally:
            if os.path.exists(lock_path):
                os.remove(lock_path)

    def _save_summary_status(self, retries=5, delay=0.1):
        """Save latest summary to a JSON file for the dashboard using atomic rename with retries."""
        try:
            path = os.path.join(self.base_dir, "data", "latest_donation_sync_status.json")
            temp_path = path + ".tmp"
            
            for i in range(retries):
                try:
                    with open(temp_path, "w", encoding="utf-8") as f:
                        json.dump(self.summary, f, indent=4)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(temp_path, path)
                    return True
                except OSError as e:
                    if e.errno == 35: # Resource deadlock avoided
                        if i < retries - 1:
                            time.sleep(delay)
                            continue
                    logging.error(f"Failed to save donation sync status (attempt {i+1}): {e}")
                    if i == retries - 1: raise
        except Exception as e:
            logging.error(f"Unexpected error saving donation sync status: {e}")
        return False

    def send_summary_email(self, fatal_error: str = None):
        """Send donation sync summary email."""
        sender_email = os.getenv("SMTP_SENDER_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")
        # Recipient priority: 1. dynamic settings, 2. env var
        recipient_email = self.settings.get("confirmation_email")
        if not recipient_email:
            recipient_email = os.getenv("SMTP_RECIPIENT_EMAIL", "")

        if not sender_email or not sender_password:
            logging.warning("Email credentials missing. Skipping donation sync email.")
            return
        if not recipient_email or recipient_email in ["admin@example.com", ""]:
            logging.warning("No valid recipient email. Skipping donation sync email.")
            return

        subject = f"QB → PCO Donation Sync - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        body = "Donation Sync Summary:\n"
        if fatal_error:
            body += f"FATAL ERROR: {fatal_error}\n\n"
        body += f"Donations Created: {self.summary['donations_created']}\n"
        body += f"Donations Skipped: {self.summary['donations_skipped']}\n"
        body += f"Errors: {self.summary['errors']}\n\n"
        body += "Logs:\n" + "\n".join(self.summary["logs"])

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = recipient_email

        smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", 587))

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)
            logging.info("Donation sync summary email sent")
        except Exception as e:
            logging.error(f"Failed to send donation sync email: {e}")
