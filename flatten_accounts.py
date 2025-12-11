"""
Flatten accounts query response from query_getAccounts.

Handles:
- serviceable_address_account_assignment_histories (array)
- contacts (array with nested phone_numbers)
- addresses (array)

For nested arrays, we flatten the FIRST entry of each array to keep one row per account.
If you need ALL entries, consider creating separate rows per contact/address/history.
"""

from typing import Dict, List, Any
import pandas as pd


def flatten_accounts_response(response: Dict[str, Any]) -> pd.DataFrame:
    """
    Flatten accounts response into a flat DataFrame.
    
    Handles:
    - Multiple accounts at top level
    - serviceable_address_account_assignment_histories array (takes first entry)
    - contacts array with nested phone_numbers (takes first contact, first phone)
    - addresses array (takes first address)
    - Null/empty nested objects filled with None
    
    Field naming:
    - Duplicate 'id' fields prefixed with parent: Account_id, AssignmentHistory_id, Contact_id, Address_id, PhoneNumber_id
    - Other fields use parent prefix where needed
    
    Args:
        response: Dict with structure {'data': {'accounts': {'entities': [...]}}}
    
    Returns:
        Flat pandas DataFrame with all extracted fields
    """
    
    flattened_data = []
    accounts = response.get('data', {}).get('accounts', {}).get('entities', [])
    
    for account in accounts:
        # Start with top-level account fields
        row = {
            'Account_id': account.get('id'),
        }
        
        # ====================================================================
        # EXTRACT: serviceable_address_account_assignment_histories (array)
        # Take last 3 entries if exists (most cases only 1, but future-proofed)
        # ====================================================================
        assignment_histories = account.get('serviceable_address_account_assignment_histories', {}).get('entities', [])
        
        # Get last 3 (or fewer if less than 3 exist)
        last_3_histories = assignment_histories[-3:] if assignment_histories else []
        
        # Initialize with None for all 3 slots
        for i in range(1, 4):
            row.update({
                f'AssignmentHistory{i}_created_at': None,
                f'AssignmentHistory{i}_end_date': None,
                f'AssignmentHistory{i}_id': None,
                f'AssignmentHistory{i}_account_id': None,
                f'AssignmentHistory{i}_address_id': None,
            })
        
        # Fill in actual data from last 3 histories (reversed order so most recent is AssignmentHistory1)
        for idx, history in enumerate(reversed(last_3_histories)):
            position = idx + 1  # 1, 2, 3
            row.update({
                f'AssignmentHistory{position}_created_at': history.get('created_at'),
                f'AssignmentHistory{position}_end_date': history.get('end_date'),
                f'AssignmentHistory{position}_id': history.get('id'),
                f'AssignmentHistory{position}_account_id': history.get('account_id'),
                f'AssignmentHistory{position}_address_id': history.get('address_id'),
            })
        
        # ====================================================================
        # EXTRACT: contacts (array with nested phone_numbers)
        # Take first contact, and first phone number if exists
        # ====================================================================
        contacts_list = account.get('contacts', {}).get('entities', [])
        
        if contacts_list:
            contact = contacts_list[0]
            row.update({
                'Contact_name': contact.get('name'),
                'Contact_role': contact.get('role'),
                'Contact_primary': contact.get('primary'),
                'Contact_email_address': contact.get('email_address'),
            })
            
            # Extract first phone number if exists
            phone_numbers = contact.get('phone_numbers', {}).get('entities', [])
            if phone_numbers:
                phone = phone_numbers[0]
                row.update({
                    'PhoneNumber_country': phone.get('country'),
                    'PhoneNumber_number': phone.get('number'),
                    'PhoneNumber_number_formatted': phone.get('number_formatted'),
                })
            else:
                row.update({
                    'PhoneNumber_country': None,
                    'PhoneNumber_number': None,
                    'PhoneNumber_number_formatted': None,
                })
        else:
            row.update({
                'Contact_name': None,
                'Contact_role': None,
                'Contact_primary': None,
                'Contact_email_address': None,
                'PhoneNumber_country': None,
                'PhoneNumber_number': None,
                'PhoneNumber_number_formatted': None,
            })
        
        # ====================================================================
        # EXTRACT: addresses (array)
        # Take first address if exists
        # ====================================================================
        addresses_list = account.get('addresses', {}).get('entities', [])
        
        if addresses_list:
            address = addresses_list[0]
            row.update({
                'Address_address_status_id': address.get('address_status_id'),
                'Address_line1': address.get('line1'),
                'Address_line2': address.get('line2'),
                'Address_city': address.get('city'),
                'Address_county': address.get('county'),
                'Address_subdivision': address.get('subdivision'),
                'Address_zip': address.get('zip'),
                'Address_country': address.get('country'),
                'Address_latitude': address.get('latitude'),
                'Address_longitude': address.get('longitude'),
                'Address_fips': address.get('fips'),
                'Address_type': address.get('type'),
                'Address_addressable_type': address.get('addressable_type'),
                'Address_addressable_id': address.get('addressable_id'),
                'Address_serviceable': address.get('serviceable'),
                'Address_id': address.get('id'),
            })
        else:
            row.update({
                'Address_address_status_id': None,
                'Address_line1': None,
                'Address_line2': None,
                'Address_city': None,
                'Address_county': None,
                'Address_subdivision': None,
                'Address_zip': None,
                'Address_country': None,
                'Address_latitude': None,
                'Address_longitude': None,
                'Address_fips': None,
                'Address_type': None,
                'Address_addressable_type': None,
                'Address_addressable_id': None,
                'Address_serviceable': None,
                'Address_id': None,
            })
        
        flattened_data.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)
    
    return df


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == '__main__':
    # Example: if you have a response from query_getAccounts
    # response = build_api_request_and_execute(query=query_getAccounts())
    # df = flatten_accounts_response(response)
    # print(df.to_string(index=False))
    # df.to_csv('accounts_flat.csv', index=False)
    
    print("flatten_accounts_response() ready to use")
    print("Pass response dict from query_getAccounts() to flatten it")
