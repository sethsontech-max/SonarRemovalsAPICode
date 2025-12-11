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
    '''
    Docstring for build_api_request_and_execute
    
    :param query: string GraphQL query
    :param payload: string JSON payload
    :return: JSON response or None on failure
    '''
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
      VARIABLES = ''' {
          "paginator": {
              "page": 1,
              "records_per_page": 10000
          }
      }'''

      # ============================================================================
      # Build the GraphQL payload
      # ============================================================================
      PAYLOAD = ''' {
          "query": QUERY,
          "variables": VARIABLES
          } '''
      return PAYLOAD



## specific query functions


def query_uninstallJobs():
    '''
    Query to retrieve uninstall jobs. Hard coded query and variables.
    '''
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


def Get_UninstallJobs_List():
    '''
    Get list of uninstall jobs with entity_type and entity_id based on query_uninstallJobs
    '''
    uninstallJobs = build_api_request_and_execute(payload=query_uninstallJobs())
    uninstall_data = []
    for job in uninstallJobs['data']['jobs']['entities']:
        uninstall_data.append({
            'entity_type': job['jobbable']['__typename'],
            'entity_id': job['jobbable']['id'],
            'reason': 'Uninstall Job'
        })

    uninstall_data = pd.DataFrame(uninstall_data)

    return uninstall_data

def query_getInventoryItems():
    
    '''
    Query to retrieve account information. Hard coded query and variables.
    '''
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
    '''
    Query to retrieve account information. Hard coded query and variables.
    '''
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


def build_reason(row, uninstall_ids):
    # Build reason string based on rules for removal
    reasons = []

    # First rule: no account assigned
    if pd.isna(row["Account_id_inv"]):
        reasons.append("inventory with no account")

    # Second rule: status not 1 or 11
    status = row["Account_account_status_id"]

    if pd.isna(status):
        pass      # or add a different reason if you want to treat null status as a problem
    else:
        if status not in (1, 11):
            reasons.append("inventory assigned to Inactive account")

    # Rule 3: account has an uninstall job
    acct = row["Account_id_inv"]
    if not pd.isna(acct):
        if acct in uninstall_ids:
            reasons.append("uninstall job")

    # Join reasons if any exist
    if reasons:
        return "; ".join(reasons)

    return None


def main():
    
    '''
    output removal list as CSV named "removal_list_filter.csv" based on criteria:
    1) inventory with no account
    2) inventory assigned to Inactive account (status not 1 or 11)
    3) inventory assigned to account with uninstall job

    information is pulled from Sonar GraphQL API
    API key and endpoint are read from secrets.env file. Update as needed.

    File dependancies:
    - flatten_inventory_items.py
    - flatten_accounts.py
    - secrets.env


    improvements to do:
    - for inventory without an account, check assignment history and/or disconnection log to determine who was last assigned the inventory. Use this information to populate account name and contact information for output.
            - will handle manually for now. Because the list will only every get smaller over time, it is unlikely to be a big issue or bothered to be implemented.
    
    '''

######## get inventory and account data as JSON responses and flatten to dataframes

    response_inventory = build_api_request_and_execute(payload=query_getInventoryItems())

    if response_inventory is None:
        print("Failed to retrieve inventory items.")
        return 
    df_inventory = flatten_inventory_items_response(response_inventory)



    response_accounts = build_api_request_and_execute(payload=query_getAccounts())
    if response_accounts is None:
        print("Failed to retrieve accounts.")
        return
    df_accounts = flatten_accounts_response(response_accounts)


  ##### end #### get inventory and account data as JSON responses and flatten to dataframes

  ####### consolidate data into a single dataframe

# ensure 'Address_id' exists in both dataframes
    for df, name in [(df_inventory, 'df_inventory'), (df_accounts, 'df_accounts')]:
        if 'Address_id' not in df.columns:
            raise KeyError(f"Required column 'Address_id' missing from {name}")
        
    # 2) Normalize Address_id types and strip whitespace (if string-like)
    df_inventory['Address_id'] = df_inventory['Address_id'].astype('string').str.strip()
    df_accounts['Address_id'] = df_accounts['Address_id'].astype('string').str.strip()

  
      # 4) If validation passes, perform LEFT merge and continue
    merged = df_inventory.merge(df_accounts, on='Address_id', how='left', suffixes=('_inv', '_acc'))

    #### end ####### consolidate data into a single dataframe


 #### create reason column based on rules and populate it #####

    merged["reason"] = None
    merged["Account_account_status_id"] = pd.to_numeric(merged["Account_account_status_id"], errors='coerce').astype('Int64')
    
    #build list of uninstall jobs account ids
    uninstall_data = Get_UninstallJobs_List()
    uninstall_ids = set(uninstall_data["entity_id"].dropna())
    #convert account_id to integer for comparison
    uninstall_ids = set(int(x) for x in uninstall_ids if str(x).isdigit())
    merged["Account_id_inv"] = pd.to_numeric(merged["Account_id_inv"], errors='coerce').astype('Int64')

    merged["reason"] = merged.apply(lambda row: build_reason(row, uninstall_ids=uninstall_ids), axis=1)

    # Output to verify result
    # merged.to_csv('removal_list_combined_Inactive.csv', index=False)
    # print("Saved 'removal_list_combined_Inactive.csv'")


    #remove duplicates based on inventory id, keeping first occurrence
    df_final = merged.drop_duplicates(subset=['InventoryItem_id'])


  #### end #### create reason column based on rules and populate it #####

  #### output desired columns to csv #####

    #create output filtered removal list with only rows that have a non-null reason
    df_filtered = df_final[df_final['reason'].notnull()]

    # reanme columns to human friendly names
    df_filtered_renamed = df_filtered.rename(columns={
        'InventoryItem_id': 'InventoryItem_id',
        'InventoryModel_model_name': 'InventoryModel',
        'FieldData_value': 'MAC',
        'IPAssignment_subnet': 'IP',
        'Address_id': 'Address_id',
        'Address_line1_inv': 'Address_line1',
        'Address_line2_inv': 'Address_line2',
        'reason': 'Reason',
        'Account_id_inv': 'Account_id',
        'Account_name_inv': 'Account_name',
        'Account_account_status_id': 'Account_status',
        'Contact_email_address': 'Account_email',
        'PhoneNumber_number_formatted': 'Account_phone',
        'AssignmentHistory1_end_date': 'end_date'
    })


    #produce output csv with selected columns

    cols = ["InventoryItem_id", "InventoryModel", "MAC", "IP", "Address_id", "Address_line1", "Address_line2", "Reason", "Account_id", "Account_name", "Account_status", "Account_email", "Account_phone", "end_date"]
    df_filtered_renamed = df_filtered_renamed[cols]
    df_filtered_renamed.to_csv('removal_list_filtered.csv', index=False)
    print(f"Saved 'removal_list_filtered.csv' with {len(df_filtered_renamed)} rows (non-null reasons)")

    # end #### output desired columns to csv #####

    return

main()