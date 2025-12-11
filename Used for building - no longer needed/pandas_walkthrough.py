"""
PANDAS WALKTHROUGH - Converting JSON to DataFrames
====================================================

This file demonstrates step-by-step how to:
1. Convert JSON to Pandas DataFrames
2. Merge DataFrames
3. Handle nested data
4. Deduplicate and aggregate
"""

import pandas as pd
import json

# ============================================================================
# STEP 1: Understanding DataFrames vs Lists/Dicts
# ============================================================================
print("\n" + "="*70)
print("STEP 1: What is a DataFrame?")
print("="*70)

# Traditional Python - List of Dicts (like JSON)
data_dict = [
    {"id": "1", "name": "Alice", "status": "Active"},
    {"id": "2", "name": "Bob", "status": "Inactive"},
    {"id": "3", "name": "Charlie", "status": "Active"}
]

# Convert to DataFrame - THIS IS THE KEY CONVERSION
df = pd.DataFrame(data_dict)

print("\nOriginal data (Python list of dicts):")
print(json.dumps(data_dict, indent=2))

print("\n\nConverted to DataFrame:")
print(df)

print("\n\nDataFrame Info:")
print(f"Shape: {df.shape}")  # (3 rows, 3 columns)
print(f"Columns: {df.columns.tolist()}")  # ['id', 'name', 'status']
print(f"Type: {type(df)}")  # <class 'pandas.core.frame.DataFrame'>


# ============================================================================
# STEP 2: Accessing DataFrame Data
# ============================================================================
print("\n" + "="*70)
print("STEP 2: Accessing DataFrame Data")
print("="*70)

print("\nAccess single column (returns Series):")
print(df['name'])

print("\n\nAccess single row (returns Series):")
print(df.loc[0])  # First row

print("\n\nAccess specific cell:")
print(f"Value at row 1, column 'name': {df.loc[1, 'name']}")

print("\n\nFilter rows (WHERE clause):")
active_only = df[df['status'] == 'Active']
print(active_only)


# ============================================================================
# STEP 3: Handling Nested JSON
# ============================================================================
print("\n" + "="*70)
print("STEP 3: Handling Nested JSON")
print("="*70)

# Complex nested JSON (like your GraphQL response)
nested_json = {
    'data': {
        'inventory': {
            'entities': [
                {
                    'id': '1',
                    'mac': 'AA:BB:CC:DD:EE:FF',
                    'model': {
                        'id': '10',
                        'name': 'Model X'
                    }
                },
                {
                    'id': '2',
                    'mac': 'AA:BB:CC:DD:EE:GG',
                    'model': {
                        'id': '11',
                        'name': 'Model Y'
                    }
                }
            ]
        }
    }
}

print("\nNested JSON structure:")
print(json.dumps(nested_json, indent=2))

# Extract the entities list
inventory_data = nested_json['data']['inventory']['entities']

# You need to FLATTEN nested data before converting to DataFrame
# Option A: Extract nested fields manually
flattened_data = []
for item in inventory_data:
    flattened_data.append({
        'inventory_id': item['id'],
        'mac': item['mac'],
        'model_id': item['model']['id'],
        'model_name': item['model']['name']
    })

df_inventory = pd.DataFrame(flattened_data)
print("\n\nFlattened DataFrame:")
print(df_inventory)


# ============================================================================
# STEP 4: Merging DataFrames (JOINs)
# ============================================================================
print("\n" + "="*70)
print("STEP 4: Merging DataFrames (SQL JOIN equivalent)")
print("="*70)

# Create two sample DataFrames to merge
df_accounts = pd.DataFrame([
    {'account_id': '100', 'account_name': 'Acme Corp', 'status': 'Active'},
    {'account_id': '101', 'account_name': 'Tech Inc', 'status': 'Inactive'}
])

df_addresses = pd.DataFrame([
    {'address_id': '1', 'account_id': '100', 'address_line': '123 Main St'},
    {'address_id': '2', 'account_id': '101', 'address_line': '456 Oak Ave'}
])

print("\nAccounts DataFrame:")
print(df_accounts)

print("\n\nAddresses DataFrame:")
print(df_addresses)

# MERGE (like SQL JOIN)
merged = pd.merge(
    df_accounts, 
    df_addresses, 
    on='account_id',  # Join on matching account_id
    how='left'        # LEFT JOIN - keep all accounts
)

print("\n\nAfter MERGE (JOIN):")
print(merged)


# ============================================================================
# STEP 5: Deduplication and Aggregation
# ============================================================================
print("\n" + "="*70)
print("STEP 5: Deduplication and Aggregation")
print("="*70)

# Sample data with duplicates
df_with_duplicates = pd.DataFrame([
    {'inventory_id': '1', 'reason': 'Inactive Account'},
    {'inventory_id': '1', 'reason': 'Uninstall Job'},
    {'inventory_id': '2', 'reason': 'Inactive Account'},
    {'inventory_id': '3', 'reason': 'Address Without Account'},
])

print("\nDataFrame with duplicate inventory IDs:")
print(df_with_duplicates)

# Combine reasons for duplicate IDs
deduplicated = df_with_duplicates.groupby('inventory_id')['reason'].apply(
    lambda x: ' | '.join(x)  # Join reasons with pipe separator
).reset_index()

print("\n\nAfter deduplication (reasons combined):")
print(deduplicated)


# ============================================================================
# STEP 6: Complete Example - Your Use Case
# ============================================================================
print("\n" + "="*70)
print("STEP 6: Complete Example - Your Removal List Use Case")
print("="*70)

# Simulated API responses (flattened from nested JSON)
inventory_response = [
    {'inventory_id': '1', 'model_id': '10', 'mac': 'AA:BB:CC:01', 'ip': '192.168.1.1'},
    {'inventory_id': '2', 'model_id': '11', 'mac': 'AA:BB:CC:02', 'ip': None},
    {'inventory_id': '3', 'model_id': '12', 'mac': 'AA:BB:CC:03', 'ip': '192.168.1.3'},
]

address_response = [
    {'address_id': '100', 'inventory_id': '1', 'line1': '123 Main St'},
    {'address_id': '101', 'inventory_id': '2', 'line1': '456 Oak Ave'},
    {'address_id': '102', 'inventory_id': '3', 'line1': '789 Pine Rd'},
]

account_response = [
    {'account_id': '1000', 'address_id': '100', 'name': 'Alice Corp', 'status': 'Active'},
    {'account_id': '1001', 'address_id': '101', 'name': 'Bob Inc', 'status': 'Inactive'},
    {'account_id': '1002', 'address_id': '102', 'name': 'Charlie LLC', 'status': 'Active'},
]

# Removal reasons (from your three filter lists)
removal_reasons = [
    {'inventory_id': '1', 'reason': 'Inactive Account'},
    {'inventory_id': '2', 'reason': 'Address Without Account'},
    {'inventory_id': '1', 'reason': 'Uninstall Job'},  # Duplicate inventory_id!
]

# Create DataFrames
df_inv = pd.DataFrame(inventory_response)
df_addr = pd.DataFrame(address_response)
df_acc = pd.DataFrame(account_response)
df_reasons = pd.DataFrame(removal_reasons)

print("\n1. Inventory DataFrame:")
print(df_inv)

print("\n2. Address DataFrame:")
print(df_addr)

print("\n3. Account DataFrame:")
print(df_acc)

print("\n4. Removal Reasons DataFrame (with duplicates):")
print(df_reasons)

# MERGE all together
result = df_inv.merge(df_addr, on='inventory_id', how='left')
result = result.merge(df_acc, on='address_id', how='left')
result = result.merge(df_reasons, on='inventory_id', how='left')

print("\n5. After all merges (BEFORE deduplication):")
print(result)

# Deduplicate by inventory_id, combining reasons
# NOTE: Need to handle NaN values (missing data) when joining reasons
final = result.groupby('inventory_id').agg({
    'model_id': 'first',
    'mac': 'first',
    'ip': 'first',
    'address_id': 'first',
    'line1': 'first',
    'account_id': 'first',
    'name': 'first',
    'status': 'first',
    'reason': lambda x: ' | '.join([str(r) for r in x if pd.notna(r)])  # Skip NaN values
}).reset_index()

print("\n6. FINAL - After deduplication with combined reasons:")
print(final)

# Export to CSV
final.to_csv('removal_list.csv', index=False)
print("\n7. Exported to 'removal_list.csv'")


# ============================================================================
# STEP 7: Key Pandas Functions Reference
# ============================================================================
print("\n" + "="*70)
print("STEP 7: Key Pandas Functions Reference")
print("="*70)

reference = """
KEY FUNCTIONS:

1. pd.DataFrame(data)
   - Convert list of dicts to DataFrame
   - data = [{'col1': val, 'col2': val}, ...]

2. df.merge(other_df, on='key', how='left')
   - JOIN operation: left, right, inner, outer
   - on='key' - which column to join
   - suffixes - for duplicate column names

3. df.groupby('column')
   - Group by column values
   - Then use .agg() or .apply()

4. df.groupby('col1')['col2'].apply(lambda x: ...)
   - Apply function to grouped values
   - lambda x: ' | '.join(x) - concatenate strings

5. df[df['column'] == 'value']
   - Filter rows (WHERE clause)

6. df.to_csv('file.csv', index=False)
   - Export to CSV

7. df.loc[row, 'column']
   - Access specific cell

8. df['column'].unique()
   - Get unique values

9. df.drop_duplicates(subset=['column'])
   - Remove exact duplicates
"""

print(reference)
