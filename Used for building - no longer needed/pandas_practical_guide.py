"""
PANDAS FOR YOUR SONAR REMOVAL LIST - PRACTICAL GUIDE
=====================================================

This shows how to build your complete removal list using pandas.
"""

import pandas as pd
import json
from typing import Dict, List, Any

# ============================================================================
# UNDERSTANDING YOUR DATA FLOW
# ============================================================================
"""
Your GraphQL API returns nested JSON like this.
We need to flatten this nested structure into DataFrames.

Key nesting levels:
- data.inventory.entities[].id, model{}, mac, ip
- data.addresses.entities[].id, serviceable_address_account_assignment_histories
- data.accounts.entities[].id, name, account_status, email, phone

Your task is to FLATTEN this and merge the tables together.
"""


# ============================================================================
# STEP 1: FLATTEN NESTED JSON TO DATAFRAMES
# ============================================================================

def flatten_inventory_response(response: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract inventory data from nested GraphQL response.
    Converts: response['data']['inventory']['entities']
    To flat DataFrame with columns: inventory_id, inventory_model_id, mac, ip, etc.
    """
    data = []
    inventory_list = response.get('data', {}).get('inventory', {}).get('entities', [])
    
    for item in inventory_list:
        data.append({
            'inventory_id': item.get('id'),
            'inventory_model_id': item.get('inventory_model_id'),
            'model_name': item.get('model', {}).get('model_name'),
            'mac': item.get('mac'),
            'ip': item.get('ip')
        })
    
    return pd.DataFrame(data)


def flatten_address_response(response: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract address data from nested GraphQL response.
    Also extracts the inventory_items relationship.
    """
    data = []
    address_list = response.get('data', {}).get('addresses', {}).get('entities', [])
    
    for addr in address_list:
        # Get inventory IDs for this address
        inventory_ids = []
        inventory_items = addr.get('inventory_items', {}).get('entities', [])
        for item in inventory_items:
            inventory_ids.append(item.get('id'))
        
        # Get account assignment history
        histories = addr.get('serviceable_address_account_assignment_histories', {}).get('entities', [])
        
        data.append({
            'address_id': addr.get('id'),
            'address_type': addr.get('type'),
            'address_line1': addr.get('line1'),
            'inventory_ids': ','.join(inventory_ids) if inventory_ids else None,
            'history_count': len(histories),
            'has_histories': len(histories) > 0
        })
    
    return pd.DataFrame(data)


def flatten_accounts_response(response: Dict[str, Any]) -> pd.DataFrame:
    """
    Extract account data from nested GraphQL response.
    """
    data = []
    account_list = response.get('data', {}).get('accounts', {}).get('entities', [])
    
    for acc in account_list:
        data.append({
            'account_id': acc.get('id'),
            'account_name': acc.get('name'),
            'account_status': acc.get('account_status', {}).get('name'),
            'email': acc.get('email'),
            'phone': acc.get('phone')
        })
    
    return pd.DataFrame(data)


# ============================================================================
# STEP 2: CREATE REMOVAL REASONS DATAFRAME
# ============================================================================

def create_removal_reasons_df(
    uninstall_inventory_ids: List[str],
    no_account_address_ids: List[str],
    inactive_account_inventory_ids: List[str]
) -> pd.DataFrame:
    """
    Combine your three filter lists into a single DataFrame with reasons.
    
    This handles duplicates naturally - if an inventory_id appears in multiple
    lists, it will have multiple rows with different reasons.
    """
    data = []
    
    # Add uninstall job reasons
    for inv_id in uninstall_inventory_ids:
        data.append({
            'inventory_id': inv_id,
            'reason': 'Uninstall Job'
        })
    
    # Add address without account reasons
    for addr_id in no_account_address_ids:
        data.append({
            'address_id': addr_id,
            'reason': 'Address Without Account'
        })
    
    # Add inactive account reasons
    for inv_id in inactive_account_inventory_ids:
        data.append({
            'inventory_id': inv_id,
            'reason': 'Inactive Account'
        })
    
    return pd.DataFrame(data)


# ============================================================================
# STEP 3: MERGE ALL DATAFRAMES TOGETHER
# ============================================================================

def build_removal_list(
    df_inventory: pd.DataFrame,
    df_addresses: pd.DataFrame,
    df_accounts: pd.DataFrame,
    df_reasons: pd.DataFrame
) -> pd.DataFrame:
    """
    Combine all DataFrames into one complete table.
    This is like performing SQL JOINs.
    """
    
    # Start with inventory (base table)
    result = df_inventory.copy()
    
    # LEFT JOIN with addresses on inventory_id
    # This connects inventory to physical locations
    result = result.merge(
        df_addresses,
        left_on='inventory_id',
        right_on='inventory_ids',  # Note: inventory_ids is comma-separated
        how='left'
    )
    
    # LEFT JOIN with accounts
    # (You'd need to match address_id to account through assignment history)
    # This is simplified - in real scenario you'd handle account assignment
    result = result.merge(
        df_accounts,
        on='account_id',
        how='left'
    )
    
    # LEFT JOIN with reasons
    result = result.merge(
        df_reasons,
        on='inventory_id',
        how='left'
    )
    
    # Filter to keep ONLY rows with a reason (remove NaN values)
    result = result[result['reason'].notna()]
    
    return result


# ============================================================================
# STEP 4: DEDUPLICATE AND COMBINE REASONS
# ============================================================================

def deduplicate_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group by inventory_id and combine multiple reasons.
    
    Before:
    inventory_id | reason1
    1            | Uninstall Job
    1            | Inactive Account
    
    After:
    inventory_id | reason
    1            | Uninstall Job | Inactive Account
    """
    
    # Group by inventory_id and aggregate
    final = df.groupby('inventory_id').agg({
        'inventory_model_id': 'first',
        'model_name': 'first',
        'mac': 'first',
        'ip': 'first',
        'address_id': 'first',
        'address_line1': 'first',
        'account_id': 'first',
        'account_name': 'first',
        'account_status': 'first',
        'email': 'first',
        'phone': 'first',
        'reason': lambda x: ' | '.join([str(r) for r in x if pd.notna(r)])
    }).reset_index()
    
    return final


# ============================================================================
# STEP 5: RENAME COLUMNS TO MATCH YOUR REQUIREMENTS
# ============================================================================

def format_final_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename and reorder columns to match your specification.
    """
    
    column_mapping = {
        'inventory_id': 'Inventory ID',
        'model_name': 'Inventory Model',
        'mac': 'MAC',
        'ip': 'IP',
        'address_id': 'Address ID',
        'address_line1': 'Physical Address',
        'reason': 'Reason on List',
        'account_id': 'Account ID',
        'account_name': 'Account Name',
        'account_status': 'Account Status',
        'email': 'Account Email',
        'phone': 'Account Phone'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Select and reorder columns in desired sequence
    final_columns = [
        'Inventory ID',
        'Inventory Model',
        'MAC',
        'IP',
        'Address ID',
        'Physical Address',
        'Reason on List',
        'Account ID',
        'Account Name',
        'Account Status',
        'Account Email',
        'Account Phone'
    ]
    
    # Only include columns that exist
    existing_columns = [col for col in final_columns if col in df.columns]
    df = df[existing_columns]
    
    return df


# ============================================================================
# COMPLETE WORKFLOW EXAMPLE
# ============================================================================

def build_complete_removal_list(
    inventory_response: Dict,
    address_response: Dict,
    account_response: Dict,
    uninstall_ids: List[str],
    no_account_ids: List[str],
    inactive_ids: List[str]
) -> pd.DataFrame:
    """
    Complete end-to-end workflow.
    """
    
    print("Step 1: Flattening nested JSON to DataFrames...")
    df_inv = flatten_inventory_response(inventory_response)
    df_addr = flatten_address_response(address_response)
    df_acc = flatten_accounts_response(account_response)
    print(f"  - Inventory: {len(df_inv)} records")
    print(f"  - Addresses: {len(df_addr)} records")
    print(f"  - Accounts: {len(df_acc)} records")
    
    print("\nStep 2: Creating removal reasons DataFrame...")
    df_reasons = create_removal_reasons_df(uninstall_ids, no_account_ids, inactive_ids)
    print(f"  - Reasons: {len(df_reasons)} records (may have duplicates)")
    
    print("\nStep 3: Merging all DataFrames...")
    df_merged = build_removal_list(df_inv, df_addr, df_acc, df_reasons)
    print(f"  - After merge: {len(df_merged)} records")
    
    print("\nStep 4: Deduplicating and combining reasons...")
    df_final = deduplicate_and_aggregate(df_merged)
    print(f"  - After dedup: {len(df_final)} unique inventory IDs")
    
    print("\nStep 5: Formatting output...")
    df_output = format_final_output(df_final)
    
    print("\nDone!")
    return df_output


# ============================================================================
# KEY CONCEPTS SUMMARY
# ============================================================================
"""
MERGING (Joining):
- pd.merge(df1, df2, on='key', how='left')
- LEFT JOIN: Keep all rows from df1
- INNER JOIN: Only matching rows
- Can merge on multiple columns or different column names

GROUPBY + AGGREGATION:
- df.groupby('col').agg({'col1': 'first', 'col2': lambda x: ' | '.join(x)})
- 'first': Take first non-null value
- 'lambda x: ...': Custom function on grouped values
- Handles duplicates by combining them

HANDLING NULL/NaN:
- pd.notna(value): Check if value is not null
- Skip NaN in aggregation: [v for v in x if pd.notna(v)]

FLATTENING:
- Extract nested data BEFORE creating DataFrame
- Build flat dict with all needed columns
- Then pd.DataFrame(list_of_dicts)
"""
