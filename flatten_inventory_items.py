"""
Flatten inventory items query response from query_getInventoryItems.

Handles nested structures with null values gracefully.
Duplicates field names are prefixed with their parent level name.
"""

from typing import Dict, List, Any
import pandas as pd


def flatten_inventory_items_response(response: Dict[str, Any]) -> pd.DataFrame:
    """
    Flatten deeply nested inventory items response into a flat DataFrame.
    
    Handles:
    - Multiple nesting levels (inventory_item > address > account > deployment_type)
    - Null/empty nested objects (e.g., addressable can be null)
    - Field name collisions (e.g., multiple 'id' fields - prepend parent name)
    - inventory_model_field_data and ip_assignments (complex nested arrays)
    
    Args:
        response: Dict with structure {'data': {'inventory_items': {'entities': [...]}}}
    
    Returns:
        Flat pandas DataFrame with all extracted fields
    """
    
    flattened_data = []
    inventory_items = response.get('data', {}).get('inventory_items', {}).get('entities', [])
    
    for item in inventory_items:
        # Start with top-level inventory item fields
        row = {
            'inventoryitemable_type': item.get('inventoryitemable_type'),
            'inventoryitemable_id': item.get('inventoryitemable_id'),
            'deployment_type_id': item.get('deployment_type_id'),
            'inventory_model_id': item.get('inventory_model_id'),
            'account_service_id': item.get('account_service_id'),
            'InventoryItem_id': item.get('id'),
            'InventoryItem_sonar_unique_id': item.get('sonar_unique_id'),
            'InventoryItem_created_at': item.get('created_at'),
            'InventoryItem_updated_at': item.get('updated_at'),
        }
        
        # ====================================================================
        # EXTRACT: inventoryitemable (Address)
        # ====================================================================
        inventoryitemable = item.get('inventoryitemable')
        
        if inventoryitemable:
            # Address fields
            row.update({
                'Address_address_status_id': inventoryitemable.get('Address_address_status_id'),
                'Address_line1': inventoryitemable.get('Address_line1'),
                'Address_line2': inventoryitemable.get('Address_line2'),
                'Address_city': inventoryitemable.get('Address_city'),
                'Address_county': inventoryitemable.get('Address_county'),
                'Address_subdivision': inventoryitemable.get('Address_subdivision'),
                'Address_zip': inventoryitemable.get('Address_zip'),
                'Address_country': inventoryitemable.get('Address_country'),
                'Address_latitude': inventoryitemable.get('Address_latitude'),
                'Address_longitude': inventoryitemable.get('Address_longitude'),
                'Address_fips': inventoryitemable.get('Address_fips'),
                'Address_type': inventoryitemable.get('Address_type'),
                'Address_addressable_type': inventoryitemable.get('Address_addressable_type'),
                'Address_addressable_id': inventoryitemable.get('Address_addressable_id'),
                'Address_serviceable': inventoryitemable.get('Address_serviceable'),
                'Address_is_anchor': inventoryitemable.get('Address_is_anchor'),
                'Address_anchor_address_id': inventoryitemable.get('Address_anchor_address_id'),
                'Address_billing_default_id': inventoryitemable.get('Address_billing_default_id'),
                'Address_attainable_download_speed': inventoryitemable.get('Address_attainable_download_speed'),
                'Address_attainable_upload_speed': inventoryitemable.get('Address_attainable_upload_speed'),
                'Address_timezone': inventoryitemable.get('Address_timezone'),
                'Address_id': inventoryitemable.get('Address_id'),
                'Address_sonar_unique_id': inventoryitemable.get('Address_sonar_unique_id'),
                'Address_created_at': inventoryitemable.get('Address_created_at'),
                'Address_updated_at': inventoryitemable.get('Address_updated_at'),
            })
            
            # ====================================================================
            # EXTRACT: addressable (Account) - nested inside Address
            # ====================================================================
            addressable = inventoryitemable.get('addressable')
            
            if addressable:
                # Account fields
                row.update({
                    'Account_name': addressable.get('Account_name'),
                    'Account_archived_at': addressable.get('Account_archived_at'),
                    'Account_archived_by_user_id': addressable.get('Account_archived_by_user_id'),
                    'Account_account_status_id': addressable.get('Account_account_status_id'),
                    'Account_account_type_id': addressable.get('Account_account_type_id'),
                    'Account_is_delinquent': addressable.get('Account_is_delinquent'),
                    'Account_is_eligible_for_archive': addressable.get('Account_is_eligible_for_archive'),
                    'Account_company_id': addressable.get('Account_company_id'),
                    'Account_activation_date': addressable.get('Account_activation_date'),
                    'Account_next_bill_date': addressable.get('Account_next_bill_date'),
                    'Account_parent_account_id': addressable.get('Account_parent_account_id'),
                    'Account_geopoint': addressable.get('Account_geopoint'),
                    'Account_data_usage_percentage': addressable.get('Account_data_usage_percentage'),
                    'Account_next_recurring_charge_amount': addressable.get('Account_next_recurring_charge_amount'),
                    'Account_disconnection_reason_id': addressable.get('Account_disconnection_reason_id'),
                    'Account_id': addressable.get('Account_id'),
                    'Account_sonar_unique_id': addressable.get('Account_sonar_unique_id'),
                    'Account_created_at': addressable.get('Account_created_at'),
                    'Account_updated_at': addressable.get('Account_updated_at'),
                })
            else:
                # addressable is null - fill with None values
                row.update({
                    'Account_name': None,
                    'Account_archived_at': None,
                    'Account_archived_by_user_id': None,
                    'Account_account_status_id': None,
                    'Account_account_type_id': None,
                    'Account_is_delinquent': None,
                    'Account_is_eligible_for_archive': None,
                    'Account_company_id': None,
                    'Account_activation_date': None,
                    'Account_next_bill_date': None,
                    'Account_parent_account_id': None,
                    'Account_geopoint': None,
                    'Account_data_usage_percentage': None,
                    'Account_next_recurring_charge_amount': None,
                    'Account_disconnection_reason_id': None,
                    'Account_id': None,
                    'Account_sonar_unique_id': None,
                    'Account_created_at': None,
                    'Account_updated_at': None,
                })
        else:
            # inventoryitemable (Address) is null - fill all nested with None
            # Address fields
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
                'Address_is_anchor': None,
                'Address_anchor_address_id': None,
                'Address_billing_default_id': None,
                'Address_attainable_download_speed': None,
                'Address_attainable_upload_speed': None,
                'Address_timezone': None,
                'Address_id': None,
                'Address_sonar_unique_id': None,
                'Address_created_at': None,
                'Address_updated_at': None,
            })
            
            # Account fields
            row.update({
                'Account_name': None,
                'Account_archived_at': None,
                'Account_archived_by_user_id': None,
                'Account_account_status_id': None,
                'Account_account_type_id': None,
                'Account_is_delinquent': None,
                'Account_is_eligible_for_archive': None,
                'Account_company_id': None,
                'Account_activation_date': None,
                'Account_next_bill_date': None,
                'Account_parent_account_id': None,
                'Account_geopoint': None,
                'Account_data_usage_percentage': None,
                'Account_next_recurring_charge_amount': None,
                'Account_disconnection_reason_id': None,
                'Account_id': None,
                'Account_sonar_unique_id': None,
                'Account_created_at': None,
                'Account_updated_at': None,
            })
        
        # ====================================================================
        # EXTRACT: deployment_type
        # ====================================================================
        deployment_type = item.get('deployment_type')
        
        if deployment_type:
            row.update({
                'DeploymentType_name': deployment_type.get('name'),
                'DeploymentType_inventory_model_id': deployment_type.get('inventory_model_id'),
                'DeploymentType_network_monitoring_template_id': deployment_type.get('network_monitoring_template_id'),
                'DeploymentType_id': deployment_type.get('id'),
            })
        else:
            row.update({
                'DeploymentType_name': None,
                'DeploymentType_inventory_model_id': None,
                'DeploymentType_network_monitoring_template_id': None,
                'DeploymentType_id': None,
            })
        
        # ====================================================================
        # EXTRACT: inventory_model
        # ====================================================================
        inventory_model = item.get('inventory_model')
        
        if inventory_model:
            row.update({
                'InventoryModel_enabled': inventory_model.get('enabled'),
                'InventoryModel_manufacturer_id': inventory_model.get('manufacturer_id'),
                'InventoryModel_inventory_model_category_id': inventory_model.get('inventory_model_category_id'),
                'InventoryModel_icon': inventory_model.get('icon'),
                'InventoryModel_model_name': inventory_model.get('model_name'),
                'InventoryModel_name': inventory_model.get('name'),
            })
        else:
            row.update({
                'InventoryModel_enabled': None,
                'InventoryModel_manufacturer_id': None,
                'InventoryModel_inventory_model_category_id': None,
                'InventoryModel_icon': None,
                'InventoryModel_model_name': None,
                'InventoryModel_name': None,
            })
        
        # ====================================================================
        # EXTRACT: inventory_model_field_data (array)
        # NOTE: We'll flatten the FIRST field_data entry. For multiple entries,
        # consider creating separate rows or storing as JSON string.
        # ====================================================================
        field_data_list = item.get('inventory_model_field_data', {}).get('entities', [])
        
        if field_data_list:
            # Take first field_data entry
            field_data = field_data_list[0]
            row.update({
                'FieldData_inventory_model_field_id': field_data.get('inventory_model_field_id'),
                'FieldData_inventory_item_id': field_data.get('inventory_item_id'),
                'FieldData_value': field_data.get('value'),
                'FieldData_id': field_data.get('id'),
                'FieldData_inventory_model_field_id_nested': field_data.get('inventory_model_field', {}).get('inventory_model_id'),
                'FieldData_name': field_data.get('inventory_model_field', {}).get('name'),
                'FieldData_type': field_data.get('inventory_model_field', {}).get('type'),
            })
            
            # Extract IP assignments from first field_data
            ip_assignments = field_data.get('ip_assignments', {}).get('entities', [])
            if ip_assignments:
                ip_assign = ip_assignments[0]
                row.update({
                    'IPAssignment_account_service_id': ip_assign.get('account_service_id'),
                    'IPAssignment_subnet': ip_assign.get('subnet'),
                    'IPAssignment_soft': ip_assign.get('soft'),
                    'IPAssignment_reference': ip_assign.get('reference'),
                    'IPAssignment_description': ip_assign.get('description'),
                    'IPAssignment_subnet_id': ip_assign.get('subnet_id'),
                    'IPAssignment_ip_pool_id': ip_assign.get('ip_pool_id'),
                    'IPAssignment_ipassignmentable_type': ip_assign.get('ipassignmentable_type'),
                    'IPAssignment_ipassignmentable_id': ip_assign.get('ipassignmentable_id'),
                    'IPAssignment_id': ip_assign.get('id'),
                    'IPAssignment_sonar_unique_id': ip_assign.get('sonar_unique_id'),
                    'IPAssignment_created_at': ip_assign.get('created_at'),
                    'IPAssignment_updated_at': ip_assign.get('updated_at'),
                })
            else:
                row.update({
                    'IPAssignment_account_service_id': None,
                    'IPAssignment_subnet': None,
                    'IPAssignment_soft': None,
                    'IPAssignment_reference': None,
                    'IPAssignment_description': None,
                    'IPAssignment_subnet_id': None,
                    'IPAssignment_ip_pool_id': None,
                    'IPAssignment_ipassignmentable_type': None,
                    'IPAssignment_ipassignmentable_id': None,
                    'IPAssignment_id': None,
                    'IPAssignment_sonar_unique_id': None,
                    'IPAssignment_created_at': None,
                    'IPAssignment_updated_at': None,
                })
        else:
            row.update({
                'FieldData_inventory_model_field_id': None,
                'FieldData_inventory_item_id': None,
                'FieldData_value': None,
                'FieldData_id': None,
                'FieldData_inventory_model_field_id_nested': None,
                'FieldData_name': None,
                'FieldData_type': None,
                'IPAssignment_account_service_id': None,
                'IPAssignment_subnet': None,
                'IPAssignment_soft': None,
                'IPAssignment_reference': None,
                'IPAssignment_description': None,
                'IPAssignment_subnet_id': None,
                'IPAssignment_ip_pool_id': None,
                'IPAssignment_ipassignmentable_type': None,
                'IPAssignment_ipassignmentable_id': None,
                'IPAssignment_id': None,
                'IPAssignment_sonar_unique_id': None,
                'IPAssignment_created_at': None,
                'IPAssignment_updated_at': None,
            })
        
        flattened_data.append(row)
    
    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)
    
    return df


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == '__main__':
    # Example: if you have a response from query_getInventoryItems
    # response = build_api_request_and_execute(query=query_getInventoryItems())
    # df = flatten_inventory_items_response(response)
    # print(df.to_string(index=False))
    # df.to_csv('inventory_items_flat.csv', index=False)
    
    print("flatten_inventory_items_response() ready to use")
    print("Pass response dict from query_getInventoryItems() to flatten it")
