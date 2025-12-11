import requests
import json
from typing import List
import os
from dotenv import load_dotenv
from tabulate import tabulate
import pandas as pd
from flatten_inventory_items import flatten_inventory_items_response
from flatten_accounts import flatten_accounts_response
from collections import defaultdict

# Load environment variables from .env file
load_dotenv("secrets.env")


## API testing functions
def test_connection():
    """Test GraphQL connection with a simple query"""
    
    # Get configuration from .env
    endpoint = os.getenv("SONAR_GRAPHQL_ENDPOINT")
    api_key = os.getenv("SONAR_API_KEY")
    
    # Validate configuration
    if not endpoint:
        print("ERROR: SONAR_GRAPHQL_ENDPOINT not set in .env file")
        return False
    
    print(f"Testing connection to: {endpoint}")
    if api_key:
        print(f"Using API key: {api_key[:10]}...")
    else:
        print("No API key set (may be required)")
    
    # Prepare the query
    query = """
    query inventory {
      inventory_models {
        entities {
          id
          model_name
          name
        }
      }
    }
    """
    
    # Set up headers
    headers = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    
    # Prepare payload
    payload = {
        "query": query
    }
    
    try:
        print("\nSending query...\n")
        
        # Make the request
        response = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=10
        )
        
        # Check HTTP status
        print(f"HTTP Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False
        
        # Parse response
        result = response.json()
        
        # Check for GraphQL errors
        if "errors" in result:
            print("GraphQL Errors:")
            print(json.dumps(result["errors"], indent=2))
            return False
        
        # Success! Display results
        print("Connection successful!\n")
        print("Inventory Models Found:")
        print("=" * 60)
        
        models = result["data"]["inventory_models"]["entities"]
        
        if not models:
            print("No inventory models found.")
        else:
            for model in models:
                print(f"\nID: {model['id']}")
                print(f"Model Name: {model['model_name']}")
                print(f"Name: {model['name']}")
                print("-" * 60)
        
        print(f"\nTotal models found: {len(models)}")
        
        return True
        
    except requests.exceptions.Timeout:
        print("Request timed out. Check your endpoint URL.")
        return False
    except requests.exceptions.ConnectionError:
        print("Connection error. Check your endpoint URL and network.")
        return False
    except json.JSONDecodeError:
        print("Invalid JSON response")
        print(f"Response: {response.text}")
        return False
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False
def execute_test_connection():
    print("=" * 60)
    print("SONAR GRAPHQL CONNECTION TEST")
    print("=" * 60)
    print()
    
    success = test_connection()
    
    print("\n" + "=" * 60)
    if success:
        print("TEST PASSED - Ready to run bulk operations!")
    else:
        print("TEST FAILED - Fix configuration and try again")
    print("=" * 60)



## API request and execution functions
def build_api_request_and_execute(query=None, payload=None):
    if query is None and payload is None:
        print("No query or payload provided")
        return None
    
    # Get configuration from .env
    endpoint = os.getenv("SONAR_GRAPHQL_ENDPOINT")
    api_key = os.getenv("SONAR_API_KEY")
    # Set up headers
    headers = {
        "Content-Type": "application/json",  
    }

    if api_key:   
        headers["Authorization"] = f"Bearer {api_key}"

    # Prepare payload
    if payload is None:
        payload = {
            "query": query
        }

    print("\nSending API Request...\n")
    # Make the request
    response = requests.post(
        endpoint,   
        headers=headers,
        json=payload,
        timeout=10
    )

    # Check HTTP status
    print(f"HTTP Status: {response.status_code}")
    if response.status_code != 200:
        print(f"Request failed with status {response.status_code}")
        print(f"Response: {response.text}")
        return None
    
    # Parse response
    result = response.json()
    # Check for GraphQL errors
    if "errors" in result:
        print("GraphQL Errors:")
        print(json.dumps(result["errors"], indent=2))
        return None
    print("Request executed successfully!\n")
    return result

## template for query functions
def query_template():
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
    query getInventoryModelIDs($paginator: Paginator) {
  inventory_items(paginator: $paginator) {
    entities {
      id
      inventoryitemable_type
      deployment_type_id
      deployment_type{
        name
      }
      inventory_model{
        id
        name
        deployment_types{
          entities{
            id
            name
          }
        }
      }
    }
  }
}
    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = {
        "paginator": {
            "page": 1,
            "records_per_page": 10000
        }
    }

    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD

## specific query functions
def query_getInactiveAccountsWithAssignedInventory():
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
   query getInactiveAccountsWithAssignedInventory($paginator: Paginator, $searchinactive: ReverseRelationFilter, $searchValidID: ReverseRelationFilter) {
    accounts(paginator: $paginator, reverse_relation_filters: [$searchinactive, $searchValidID]) {
    entities {
      id
      name
      account_status {
        activates_account
      }
      addresses {
        entities {
          type
          inventory_items {
            entities {
              id
              __typename
              inventory_model_id
            }
          }
        }
      }
    }
  }
}

    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = """
    {
        "searchinactive": {
            "relation": "account_status",
            "group": "1",
            "search": [
            {
                "boolean_fields": [
                {
                    "attribute": "activates_account",
                    "search_value": false
                }
                ]
            }
            ]},
            
        "searchValidID": {  
            "relation": "addresses.inventory_items",
            "group": "1",
            "search": [
            {
                "exists": "id"
            }
            ]
        },
          "paginator": {
            "page": 1,
            "records_per_page": 10000
        }

    }
    """
    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD

def query_servicableAddressesWithInventory():

    ### this will get all servicable addresses with inventory assigned to them. Then we will filter out those with active accounts assigned to them in later
    #filtering will be to find those with Servicable_address_account_assignment_histories is NULL. For those with histories, find those whose end date is exists (is not NULL)
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
    query getServicableAddressWithNoAccountAndInventoryAssigned($paginator: Paginator, $findPhysicalAddress: Search, $inventoryexist: ReverseRelationFilter){
    addresses(paginator: $paginator, search: [$findPhysicalAddress], reverse_relation_filters: [$inventoryexist])
    {
        entities{
        id
        __typename
        type
        serviceable
        serviceable_address_account_assignment_histories{
            entities{
            id
            created_at
            end_date
            account{
                name
                account_status
            {
                name
            }
            }
            }
        }
        inventory_items{
            entities{
            id
            inventory_model_id
            }
        }
        
            }
        }
        }

    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = '''
    {
        "paginator": {
            "page": 1,
            "records_per_page": 10000
        },
            "inventoryexist": {
            "relation": "inventory_items",
            "search": [
            {
                "exists" : "id"
            }
            ]
        },
        
            "findPhysicalAddress": {
            "string_fields": [
                {
                "attribute": "type",
                "search_value": "PHYSICAL",
                "match": true,
                "partial_matching": true
                }
            ]
        }
    }
    '''

    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD

def filterServicableAddressesWithoutActiveAccounts(data):
    filtered_addresses = []
    
    addresses = data['data']['addresses']['entities']
    
    for address in addresses:
        histories = address.get('serviceable_address_account_assignment_histories', {}).get('entities', [])
        inventory_items = address.get('inventory_items', {}).get('entities', [])
        inventory_ids = [item['id'] for item in inventory_items] if inventory_items else []
        
        # Case 1: histories is NULL or empty
        if not histories:
            filtered_addresses.append({
                'id': address['id'],
                'type': address['type'],
                'inventory_ids': ', '.join(inventory_ids) if inventory_ids else 'None',
                'reason': 'NULL histories'
            })
        else:
            # Check if ANY history is missing account data
            any_no_account = any(
                h.get('account') is None 
                for h in histories
            )
            
            if any_no_account:
                # Case 2: Any account is missing
                filtered_addresses.append({
                    'id': address['id'],
                    'type': address['type'],
                    'inventory_ids': ', '.join(inventory_ids) if inventory_ids else 'None',
                    'reason': 'Any account missing'
                })
            else:
                # Case 3: Check if any history has account_status.name that is NOT "Active"
                has_inactive = any(
                    h.get('account', {}).get('account_status', {}).get('name') != 'Active' 
                    for h in histories
                )
                if has_inactive:
                    filtered_addresses.append({
                        'id': address['id'],
                        'type': address['type'],
                        'inventory_ids': ', '.join(inventory_ids) if inventory_ids else 'None',
                        'reason': 'Has inactive account'
                    })
    return filtered_addresses

def query_uninstallJobs():
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
query getUninstallJobs($paginator: Paginator, $uncompleteJob: Search, $JobTypeUninstall: ReverseRelationFilter)
{
  jobs(paginator: $paginator, search: [$uncompleteJob], reverse_relation_filters: [$JobTypeUninstall]){
    entities{
      complete
      job_type
    {
      name
    }
      jobbable{
        __typename
        id
      }
    }
  }
}

    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = '''
    {
        "paginator": {
            "page": 1,
            "records_per_page": 10000
        },
          "uncompleteJob":
  {
        "boolean_fields": [
          {
            "attribute": "complete",
            "search_value": false
          }
        ]
      },
  
  "JobTypeUninstall" :
  {
  	"relation": "job_type",
    "group": "1",
    "search": [
      {
        "string_fields": [
          {
            "attribute": "name",
            "search_value": "Uninstall",
            "match": true,
            "partial_matching": false
          }
        ]
      }
    ]
    
	}

    }
    '''

    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD

def removals_lists():

    # create inactive accounts with inventory assigned list
    inactiveAccountsWithInventory = build_api_request_and_execute(payload=query_getInactiveAccountsWithAssignedInventory())
    inactiveAccout_data = []
    for account in inactiveAccountsWithInventory['data']['accounts']['entities']:
        inactiveAccout_data.append({
            'account_id': account['id'],
            'name': account['name'],
            'reason': 'Inactive Account with Inventory Assigned'
        })

    inactiveAccout_data = pd.DataFrame(inactiveAccout_data)

    # create servicable addresses without active accounts assigned list
    ServicableAccountsWithInventory = build_api_request_and_execute(payload=query_servicableAddressesWithInventory())
    filtered_addresses = filterServicableAddressesWithoutActiveAccounts(ServicableAccountsWithInventory)
    servicable_data = []
    for address in filtered_addresses:
        servicable_data.append({
            'address_id': address['id'],
            'type': address['type'],
            'inventory_ids': address['inventory_ids'],
            'reason': address['reason']
        })

    servicable_data = pd.DataFrame(servicable_data)


    # create uninstall jobs list
    uninstallJobs = build_api_request_and_execute(payload=query_uninstallJobs())
    uninstall_data = []
    for job in uninstallJobs['data']['jobs']['entities']:
        uninstall_data.append({
            'entity_type': job['jobbable']['__typename'],
            'entity_id': job['jobbable']['id'],
            'reason': 'Uninstall Job'
        })

    uninstall_data = pd.DataFrame(uninstall_data)

    return (inactiveAccout_data, servicable_data, uninstall_data)

def query_getInventoryItems():
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
    query sonarQuery($inventory_items_InventoryItem_1_paginator: Paginator, $inventory_items_InventoryItem_1_search: Search) {
  inventory_items (paginator: $inventory_items_InventoryItem_1_paginator, search: [$inventory_items_InventoryItem_1_search]) {
    entities {
      inventoryitemable {
        ... on Address {
          addressable {
            ... on Account {
              Account_name: name
              Account_archived_at: archived_at
              Account_archived_by_user_id: archived_by_user_id
              Account_account_status_id: account_status_id
              Account_account_type_id: account_type_id
              Account_is_delinquent: is_delinquent
              Account_is_eligible_for_archive: is_eligible_for_archive
              Account_company_id: company_id
              Account_activation_date: activation_date
              Account_next_bill_date: next_bill_date
              Account_parent_account_id: parent_account_id
              Account_geopoint: geopoint
              Account_data_usage_percentage: data_usage_percentage
              Account_next_recurring_charge_amount: next_recurring_charge_amount
              Account_disconnection_reason_id: disconnection_reason_id
              Account_id: id
              Account_sonar_unique_id: sonar_unique_id
              Account_created_at: created_at
              Account_updated_at: updated_at
              __typename
            }
          }
          Address_address_status_id: address_status_id
          Address_line1: line1
          Address_line2: line2
          Address_city: city
          Address_county: county
          Address_subdivision: subdivision
          Address_zip: zip
          Address_country: country
          Address_latitude: latitude
          Address_longitude: longitude
          Address_fips: fips
          Address_type: type
          Address_addressable_type: addressable_type
          Address_addressable_id: addressable_id
          Address_serviceable: serviceable
          Address_is_anchor: is_anchor
          Address_anchor_address_id: anchor_address_id
          Address_billing_default_id: billing_default_id
          Address_attainable_download_speed: attainable_download_speed
          Address_attainable_upload_speed: attainable_upload_speed
          Address_timezone: timezone
          Address_id: id
          Address_sonar_unique_id: sonar_unique_id
          Address_created_at: created_at
          Address_updated_at: updated_at
          __typename
        }
        __typename
      }

      inventoryitemable_type
      inventoryitemable_id
      deployment_type_id
      inventory_model_id
      account_service_id
      id
      sonar_unique_id
      created_at
      updated_at
      inventory_model {
        enabled
        manufacturer_id
        inventory_model_category_id
        icon
        model_name
        name
        inventory_model_fields {
          entities {
            name
            type
            secondary_type
            required
            primary
            unique
            id
            }
            __typename
          }
          __typename
        }
      deployment_type {
        name
        inventory_model_id
        network_monitoring_template_id
        id
      }
      

      inventory_model_field_data{
        entities {
          inventory_model_field_id
          inventory_item_id
          value
          id
          inventory_model_field {
            inventory_model_id
            name
            type
            secondary_type
            required
            primary
            unique
            regexp
            id
            __typename
          }
          ip_assignments{
            entities {
              ipassignmentable {
                ... on InventoryItem {
                  InventoryItem_inventoryitemable_type: inventoryitemable_type
                  InventoryItem_inventoryitemable_id: inventoryitemable_id
                  InventoryItem_deployment_type_id: deployment_type_id
                  InventoryItem_inventory_model_id: inventory_model_id
                  InventoryItem_account_service_id: account_service_id
                  InventoryItem_icmp_device_status: icmp_device_status
                  InventoryItem_icmp_status_last_change: icmp_status_last_change
                  InventoryItem_icmp_threshold_violation: icmp_threshold_violation
                  InventoryItem_snmp_device_status: snmp_device_status
                  InventoryItem_snmp_status_message: snmp_status_message
                  InventoryItem_snmp_status_last_change: snmp_status_last_change
                  InventoryItem_overall_status: overall_status
                  InventoryItem_purchase_price: purchase_price
                  InventoryItem_purchase_order_item_id: purchase_order_item_id
                  InventoryItem_status: status
                  InventoryItem_latitude: latitude
                  InventoryItem_longitude: longitude
                  InventoryItem_parent_inventory_item_id: parent_inventory_item_id
                  InventoryItem_preseem_status: preseem_status
                  InventoryItem_quantity: quantity
                  InventoryItem_um_price: um_price
                  InventoryItem_segment_parent_id: segment_parent_id
                  InventoryItem_claimed_user_id: claimed_user_id
                  InventoryItem_id: id
                  InventoryItem_sonar_unique_id: sonar_unique_id
                  InventoryItem_created_at: created_at
                  InventoryItem_updated_at: updated_at
                  __typename
                }
                ... on InventoryModelFieldData {
                  InventoryModelFieldData_inventory_model_field_id: inventory_model_field_id
                  InventoryModelFieldData_inventory_item_id: inventory_item_id
                  InventoryModelFieldData_value: value
                  InventoryModelFieldData_id: id
                  InventoryModelFieldData_sonar_unique_id: sonar_unique_id
                  InventoryModelFieldData_created_at: created_at
                  InventoryModelFieldData_updated_at: updated_at
                  __typename
                }
                __typename
              }
              account_service_id
              subnet
              soft
              reference
              description
              subnet_id
              ip_pool_id
              ipassignmentable_type
              ipassignmentable_id
              id
              sonar_unique_id
              created_at
              updated_at
            }

          }

        }

      }

    }

  }
}
    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = '''
    {
  "inventory_items_InventoryItem_1_paginator": {
    "page": 1,
    "records_per_page": 10000
  },
  "inventory_items_InventoryItem_1_search":
    {
      "string_fields": [
        {
          "attribute": "inventoryitemable_type",
          "search_value": "Address",
          "match": true
        }
      ]
    }
    }
    '''

    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD


def query_getAccounts():
    # ============================================================================
    # GraphQL Query (with variable placeholders)
    # ============================================================================
    QUERY = """
            query accounts($paginator: Paginator)
            {
            accounts(paginator: $paginator){
                entities{
                id
                serviceable_address_account_assignment_histories{
                    entities{
                    created_at
                    end_date
                    id
                    account_id
                    address_id
                    }
                }
                contacts{
                    entities{
                    name
                    role
                    primary
                    email_address
                    phone_numbers{
                        entities{
                        country
                        number
                        number_formatted
                        
                        }
                    }
                    }
                }
                addresses{
                entities{
                    address_status_id
                    line1
                    line2
                    city
                    county
                    subdivision
                    zip
                    country
                    latitude
                    longitude
                    fips
                    type
                    addressable_type
                    addressable_id
                    serviceable
                                id
                }
                } 
                }
            }
            }
    """

    # ============================================================================
    # Variables (JSON objects that will be passed to the query)
    # ============================================================================
    VARIABLES = '''
    {
        "paginator": {
            "page": 1,
            "records_per_page": 10000
        }
    }
    '''
    # ============================================================================
    # Build the GraphQL payload
    # ============================================================================
    PAYLOAD = {
        "query": QUERY,
        "variables": VARIABLES
    }
    return PAYLOAD


def combine_removal_lists(df_inventory: pd.DataFrame,
                          df_accounts: pd.DataFrame,
                          inactive_df: pd.DataFrame,
                          servicable_df: pd.DataFrame,
                          uninstall_df: pd.DataFrame) -> pd.DataFrame:
    """Combine three removal lists into a single inventory-keyed DataFrame.

    Rules:
    - Inactive entries are account-keyed and are mapped to inventory items via
      the `Account_id` -> [InventoryItem_id] mapping built from `df_inventory`.
    - Servicable rows may contain `inventory_id` or comma-separated `inventory_ids`.
    - Uninstall jobs are account-keyed ONLY (per project clarification). Non-account
      uninstall entries are logged and skipped.
    - Reasons for the same inventory are deduplicated and concatenated with ` | `.
    """
    # Normalize Account_id in both dataframes to string for reliable joins/lookups
    if 'Account_id' in df_inventory.columns:
      df_inventory['Account_id'] = df_inventory['Account_id'].astype('string').str.strip()
    if 'Account_id' in df_accounts.columns:
      df_accounts['Account_id'] = df_accounts['Account_id'].astype('string').str.strip()

    # Helper: build account -> [inventory_ids] map from inventory DataFrame
    account_to_inventories = (
      df_inventory.dropna(subset=['Account_id'])
            .groupby('Account_id')['InventoryItem_id']
            .apply(list)
            .to_dict()
    )

    def expand_servicable_row_to_ids(row):
        ids = []
        if 'inventory_id' in row and pd.notna(row.get('inventory_id')):
            ids.append(str(row['inventory_id']))
        if 'inventory_ids' in row and pd.notna(row.get('inventory_ids')):
            parts = [p.strip() for p in str(row['inventory_ids']).split(',') if p.strip()]
            ids.extend(parts)
        return ids

    inventory_reasons = defaultdict(set)

    # 1) servicable -> inventory ids
    for _, r in servicable_df.iterrows():
        inv_ids = expand_servicable_row_to_ids(r)
        reason = r.get('reason', 'Servicable Address')
        for iid in inv_ids:
            inventory_reasons[iid].add(reason)

    # 2) inactive accounts -> map to inventories
    for _, r in inactive_df.iterrows():
      acct = r.get('account_id')
      # normalize key to string to match account_to_inventories keys
      if pd.notna(acct):
        acct = str(acct).strip()
        reason = r.get('reason', 'Inactive Account with Inventory Assigned')
        invs = account_to_inventories.get(acct)
        if invs:
            for iid in invs:
                inventory_reasons[iid].add(reason)
        else:
            print(f"Note: inactive account {acct} has no inventory mapped; skipping")

    # 3) uninstall jobs -> account-keyed only
    for _, r in uninstall_df.iterrows():
      etype = str(r.get('entity_type') or '').lower()
      eid = r.get('entity_id')
      reason = r.get('reason', 'Uninstall Job')
      if 'account' in etype:
        # normalize eid key
        eid_key = str(eid).strip() if pd.notna(eid) else None
        invs = account_to_inventories.get(eid_key) or []
        if invs:
          for iid in invs:
            inventory_reasons[iid].add(reason)
        else:
          print(f"Note: uninstall job account {eid_key} has no inventory mapped; skipping")
      else:
        print(f"Warning: uninstall job entity_type='{etype}' not account-keyed; skipping entity_id={eid}")

    # Assemble final rows by looking up inventory rows and optional account fields
    final_rows = []
    for iid, reasons in inventory_reasons.items():
        inv_row = df_inventory[df_inventory['InventoryItem_id'] == iid]
        if inv_row.empty:
            final_rows.append({
                'InventoryItem_id': iid,
                'InventoryModel': None,
                'MAC': None,
                'IP': None,
                'Address_id': None,
                'Address_line1': None,
                'Reason on List': ' | '.join(sorted(reasons)),
                'Account_id': None,
                'Account_name': None,
                'Account_status': None,
                'Account_email': None,
                'Account_phone': None,
                'end_date': None,
            })
            continue

        inv = inv_row.iloc[0]
        acct_id = inv.get('Account_id')
        # normalize acct_id for lookup
        acct_key = str(acct_id).strip() if pd.notna(acct_id) else None

        acc_row = df_accounts[df_accounts['Account_id'] == acct_key] if (acct_key is not None and 'Account_id' in df_accounts.columns) else pd.DataFrame()
        account_name = None
        account_email = None
        account_phone = None
        end_date = None
        if not acc_row.empty:
          if 'Account_name' in acc_row.columns:
            account_name = acc_row.iloc[0].get('Account_name')
          if 'Contact_email_address' in acc_row.columns:
            account_email = acc_row.iloc[0].get('Contact_email_address')
          if 'PhoneNumber_number_formatted' in acc_row.columns:
            account_phone = acc_row.iloc[0].get('PhoneNumber_number_formatted')
          if 'AssignmentHistory1_end_date' in acc_row.columns:
            end_date = acc_row.iloc[0].get('AssignmentHistory1_end_date')

        # fallback: prefer inventory-side Account_name/status if account table missing info
        if not account_name:
          account_name = inv.get('Account_name')
        account_status = inv.get('Account_account_status_id') or inv.get('Account_account_status') if isinstance(inv, dict) else inv.get('Account_account_status_id')

        final_rows.append({
            'InventoryItem_id': iid,
            'InventoryModel': inv.get('InventoryModel_model_name') or inv.get('InventoryModel_name'),
            'MAC': inv.get('FieldData_value'),
            'IP': inv.get('IPAssignment_subnet'),
            'Address_id': inv.get('Address_id') or inv.get('Address_id'),
            'Address_line1': inv.get('Address_line1'),
            'Reason on List': ' | '.join(sorted(reasons)),
            'Account_id': acct_id,
            'Account_name': account_name,
            'Account_status': account_status,
            'Account_email': account_email,
            'Account_phone': account_phone,
            'end_date': end_date,
        })

    df_final = pd.DataFrame(final_rows)
    return df_final


# flatten the query results into a list of dictionaries and convert to DataFrame




def main():


#     inactiveAccountsWithInventory = build_api_request_and_execute(payload=query_getInactiveAccountsWithAssignedInventory())


# # print inactive accounts with inventory in table format for cross reference
#     table_data = []
#     for account in inactiveAccountsWithInventory['data']['accounts']['entities']:
#         table_data.append([account['id'], account['name']])

#     # Print as table
#     headers = ["Account ID", "Name"]
#     print(tabulate(table_data, headers=headers, tablefmt="grid"))



#filtering will be to find those with Servicable_address_account_assignment_histories is NULL. 
# For those with histories, find those whom do not have an "active" account status

    # ServicableAccountsWithInventory = build_api_request_and_execute(payload=query_servicableAddressesWithInventory())
    # # print(ServicableAccountsWithInventory)

    # filtered_addresses = filterServicableAddressesWithoutActiveAccounts(ServicableAccountsWithInventory)
    # # print filtered addresses
    # table_data = []
    # for address in filtered_addresses:
    #     table_data.append([address['id'], address['type'], address['inventory_ids'], address['reason']])
    # # Print as table
    # headers = ["Address ID", "Type", "Inventory IDs", "Reason"]
    # print(tabulate(table_data, headers=headers, tablefmt="grid"))


## get all uninstall jobs that are not complete
    # uninstallJobs = build_api_request_and_execute(payload=query_uninstallJobs())
    # #output uninstall jobs in table format with listing account type and id with reason = uninstall Job
    # table_data = []
    # for job in uninstallJobs['data']['jobs']['entities']:
    #     table_data.append([job['jobbable']['__typename'], job['jobbable']['id'], 'Uninstall Job'])

    # # Print as table
    # headers = ["Entity Type", "Account ID", "Reason"]
    # print(tabulate(table_data, headers=headers, tablefmt="grid"))

#now we need to create list of removals by combining all three lists and deduplicating by inventory ID
    inactiveAccout_data, servicable_data, uninstall_data = removals_lists()
    # print(pd.DataFrame(servicable_data).to_string())

    response_inventory = build_api_request_and_execute(payload=query_getInventoryItems())

    if response_inventory is None:
        print("Failed to retrieve inventory items.")
        return 
    df_inventory = flatten_inventory_items_response(response_inventory)
    # print(df_inventory.to_string(index=False))
    # df_inventory.to_csv('inventory_items_flat.csv', index=False)


    response_accounts = build_api_request_and_execute(payload=query_getAccounts())
    if response_accounts is None:
        print("Failed to retrieve accounts.")
        return
    df_accounts = flatten_accounts_response(response_accounts)
    # print(df_accounts.to_string(index=False))
    # df_accounts.to_csv('accounts_flat.csv', index=False)

    for df, name in [(df_inventory, 'df_inventory'), (df_accounts, 'df_accounts')]:
        if 'Address_id' not in df.columns:
            raise KeyError(f"Required column 'Address_id' missing from {name}")
        
    # 2) Normalize Address_id types and strip whitespace (if string-like)
    df_inventory['Address_id'] = df_inventory['Address_id'].astype('string').str.strip()
    df_accounts['Address_id'] = df_accounts['Address_id'].astype('string').str.strip()

  
      # 4) If validation passes, perform LEFT merge and continue
    merged = df_inventory.merge(df_accounts, on='Address_id', how='left', suffixes=('_inv', '_acc'))

    # # 5) Inspect or save
    # print(f"Merged rows: {len(merged)}")
    # print(merged.head(20).to_string(index=False))   # or use df.to_string(index=False) to avoid truncation
    # merged.to_csv('merged_inventory_accounts_by_address.csv', index=False)

    # Combine the three removal lists into a single inventory-keyed deduplicated table
    try:
      df_final = combine_removal_lists(df_inventory, df_accounts, inactiveAccout_data, servicable_data, uninstall_data)
      print(f"Final removal rows: {len(df_final)}")
      print(df_final.head(40).to_string(index=False))
      df_final.to_csv('removal_list_combined.csv', index=False)
      print("Saved 'removal_list_combined.csv'")
    except Exception as e:
      print(f"Failed to combine removal lists: {e}")


    return

main()