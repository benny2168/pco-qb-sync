import os
import json
import requests
import uuid
from dotenv import load_dotenv

load_dotenv()

def get_access_token():
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': os.getenv('QB_REFRESH_TOKEN')
    }
    response = requests.post(
        token_url,
        data=payload,
        auth=(os.getenv('QB_CLIENT_ID'), os.getenv('QB_CLIENT_SECRET'))
    )
    response.raise_for_status()
    return response.json()['access_token']

def test_custom_field_roundtrip():
    access_token = get_access_token()
    realm_id = os.getenv('QB_REALM_ID')
    base_url = "https://sandbox-quickbooks.api.intuit.com"
    minorversion = 70
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # 1. Create a customer with custom fields using the same names as sync_pc_to_qb.py
    unique_name = f"Test Lab {uuid.uuid4().hex[:6]}"
    payload = {
        "DisplayName": unique_name,
        "CustomField": [
            {
                "DefinitionId": "1",
                "Name": "External ID",
                "Type": "StringType",
                "StringValue": "999999"
            },
            {
                "DefinitionId": "2",
                "Name": "Nickname",
                "Type": "StringType",
                "StringValue": "LabRat"
            },
            {
                "DefinitionId": "3",
                "Name": "PrayerGroup",
                "Type": "StringType",
                "StringValue": "Lab Group"
            }
        ]
    }
    
    print(f"Creating customer '{unique_name}'...")
    create_url = f"{base_url}/v3/company/{realm_id}/customer?minorversion={minorversion}"
    response = requests.post(create_url, headers=headers, json=payload)
    if not response.ok:
        print(f"Create failed: {response.text}")
        return
        
    customer = response.json()['Customer']
    customer_id = customer['Id']
    print(f"Created customer ID: {customer_id}")
    print("\nCustomField array in CREATE response:")
    print(json.dumps(customer.get('CustomField', []), indent=2))
    
    # 2. Read it back
    print(f"\nReading back customer {customer_id}...")
    read_url = f"{base_url}/v3/company/{realm_id}/customer/{customer_id}?minorversion={minorversion}"
    response = requests.get(read_url, headers=headers)
    customer_read = response.json()['Customer']
    
    print("\nCustomField array in READ response:")
    print(json.dumps(customer_read.get('CustomField', []), indent=2))
    
    # 3. Cleanup (optional but good)
    print(f"\nDeactivating test customer {customer_id}...")
    payload_delete = {
        "Id": customer_id,
        "SyncToken": customer_read['SyncToken'],
        "sparse": True,
        "Active": False
    }
    requests.post(create_url, headers=headers, json=payload_delete)

if __name__ == "__main__":
    test_custom_field_roundtrip()
