"""
Mapping approach example for combining removal lists into a single inventory-keyed table.

This example creates small in-memory sample data and walks through:
  - Building account -> [inventory_id] map
  - Expanding servicable rows that may contain comma-separated inventory IDs
  - Mapping inactive-account and uninstall-account lists to inventory IDs using the map
  - Collecting reasons per inventory ID (deduplicated) and assembling final rows

Run with your venv: 
  .\.\venv\Scripts\Activate.ps1
  python mapping_example.py

"""

import pandas as pd
from collections import defaultdict

# -------------------------
# Sample data (small)
# -------------------------
# Flattened inventory rows (what flatten_inventory_items_response would produce)
# Note: InventoryItem_id is the canonical inventory id in this example
inventory_rows = [
    {'InventoryItem_id': 'inv1', 'Account_id': 'acct1', 'Address_id': 'addr1', 'mac': 'AA:BB:01', 'InventoryModel': 'ModelA'},
    {'InventoryItem_id': 'inv2', 'Account_id': 'acct1', 'Address_id': 'addr1', 'mac': 'AA:BB:02', 'InventoryModel': 'ModelA'},
    {'InventoryItem_id': 'inv3', 'Account_id': 'acct2', 'Address_id': 'addr2', 'mac': 'AA:BB:03', 'InventoryModel': 'ModelB'},
    {'InventoryItem_id': 'inv4', 'Account_id': None,   'Address_id': 'addr3', 'mac': 'AA:BB:04', 'InventoryModel': 'ModelC'},
]

df_inventory = pd.DataFrame(inventory_rows)

# Flattened accounts (what flatten_accounts_response would produce)
accounts_rows = [
    {'Account_id': 'acct1', 'Account_name': 'Alpha', 'Address_id': 'addr1', 'Contact_email_address': 'alpha@example.com'},
    {'Account_id': 'acct2', 'Account_name': 'Beta',  'Address_id': 'addr2', 'Contact_email_address': 'beta@example.com'},
]

df_accounts = pd.DataFrame(accounts_rows)

# Removal lists (three lists) - they are small examples of the formats you described
# 1) inactive accounts: keyed by account_id
inactive_data = [
    {'account_id': 'acct1', 'reason': 'Inactive Account with Inventory Assigned'},
    {'account_id': 'acctX', 'reason': 'Inactive Account with Inventory Assigned - no inventory'},
]
inactive_df = pd.DataFrame(inactive_data)

# 2) servicable_data: keyed by inventory_id OR inventory_ids (comma list). We'll show comma list example
servicable_data = [
    {'inventory_ids': 'inv2,inv3', 'type': 'PHYSICAL', 'reason': 'Address Without Account'},
    {'inventory_id': 'inv4', 'type': 'PHYSICAL', 'reason': 'NULL histories'},
]
servicable_df = pd.DataFrame(servicable_data)

# 3) uninstall jobs: can be account-keyed or inventory-keyed
uninstall_data = [
    {'entity_type': 'Account', 'entity_id': 'acct2', 'reason': 'Uninstall Job'},
    {'entity_type': 'InventoryItem', 'entity_id': 'inv1', 'reason': 'Uninstall Job'},
]
uninstall_df = pd.DataFrame(uninstall_data)

# -------------------------
# 1) Build account -> inventories map
# -------------------------
# Group inventory rows by Account_id to list inventory items per account
# This lets us map account-keyed removal entries to each inventory item owned by the account.
account_to_inventories = (
    df_inventory.dropna(subset=['Account_id'])
                .groupby('Account_id')['InventoryItem_id']
                .apply(list)
                .to_dict()
)

print('\nAccount -> inventories map:')
for acct, invs in account_to_inventories.items():
    print(f"  {acct} -> {invs}")

# -------------------------
# 2) Expand servicable rows to inventory ids
# -------------------------
# We normalize each servicable row to zero or more inventory IDs so all removal sources use the same keyspace

def expand_servicable_row_to_ids(row):
    ids = []
    if 'inventory_id' in row and pd.notna(row.get('inventory_id')):
        ids.append(str(row['inventory_id']))
    if 'inventory_ids' in row and pd.notna(row.get('inventory_ids')):
        parts = [p.strip() for p in str(row['inventory_ids']).split(',') if p.strip()]
        ids.extend(parts)
    return ids

print('\nExpanding servicable rows:')
for _, r in servicable_df.iterrows():
    print(' ', r.to_dict(), '->', expand_servicable_row_to_ids(r))

# -------------------------
# 3) Collect reasons per inventory id (use a set to dedupe reasons)
# -------------------------
inventory_reasons = defaultdict(set)

# servicable list -> inventory ids
for _, r in servicable_df.iterrows():
    inv_ids = expand_servicable_row_to_ids(r)
    reason = r.get('reason', 'Address Without Account')
    for iid in inv_ids:
        inventory_reasons[iid].add(reason)

# inactive accounts -> map to inventories using account_to_inventories
for _, r in inactive_df.iterrows():
    acct = r.get('account_id')
    reason = r.get('reason', 'Inactive Account with Inventory Assigned')
    invs = account_to_inventories.get(acct)
    if invs:
        for iid in invs:
            inventory_reasons[iid].add(reason)
    else:
        # No inventory found for this account: decide policy.
        # For this example we just print a log and skip.
        print(f"Note: account {acct} has no inventory mapped; skipping for inventory mapping")

# uninstall jobs -> SONAR: these are account-keyed only.
# Map uninstall jobs to inventories via the account->inventories map.
# If an uninstall entry is not account-keyed, log and skip (per clarification).
for _, r in uninstall_df.iterrows():
    etype = str(r.get('entity_type') or '').lower()
    eid = str(r.get('entity_id'))
    reason = r.get('reason', 'Uninstall Job')
    if 'account' in etype:
        invs = account_to_inventories.get(eid) or []
        if invs:
            for iid in invs:
                inventory_reasons[iid].add(reason)
        else:
            print(f"Note: uninstall job account {eid} has no inventory mapped; skipping for inventory mapping")
    else:
        # According to project rules, uninstall jobs are only associated with accounts.
        # If we encounter a non-account entity_type, warn and skip to avoid incorrect mapping.
        print(f"Warning: uninstall job entity_type='{etype}' not account-keyed; expected account-only. Skipping entity_id={eid}")

print('\nCollected reasons per inventory id (deduped):')
for iid, reasons in inventory_reasons.items():
    print(f"  {iid} -> {sorted(reasons)}")

# -------------------------
# 4) Assemble final rows by looking up inventory rows (and optional account fields)
# -------------------------
final_rows = []
for iid, reasons in inventory_reasons.items():
    # Find inventory row in df_inventory
    inv_row = df_inventory[df_inventory['InventoryItem_id'] == iid]
    if inv_row.empty:
        # missing inventory details -> create minimal placeholder
        final_rows.append({
            'InventoryItem_id': iid,
            'InventoryModel': None,
            'MAC': None,
            'Address_id': None,
            'Reason on List': ' | '.join(sorted(reasons)),
            'Account_id': None,
            'Account_name': None,
        })
        continue

    inv = inv_row.iloc[0]
    acct_id = inv.get('Account_id')
    # look up account info (df_accounts may have fewer rows)
    acc_row = df_accounts[df_accounts['Account_id'] == acct_id]
    account_name = acc_row.iloc[0]['Account_name'] if (not acc_row.empty) else None

    final_rows.append({
        'InventoryItem_id': iid,
        'InventoryModel': inv.get('InventoryModel'),
        'MAC': inv.get('mac'),
        'Address_id': inv.get('Address_id'),
        'Reason on List': ' | '.join(sorted(reasons)),
        'Account_id': acct_id,
        'Account_name': account_name,
    })

# Create final DataFrame
df_final = pd.DataFrame(final_rows)
print('\nFinal deduplicated table:')
print(df_final.to_string(index=False))

# Save output (optional)
# df_final.to_csv('example_removal_list.csv', index=False)

# -------------------------
# End of example
# -------------------------
print('\nExample complete. This demonstrates mapping-based approach for merging reasons into inventory-keyed rows.')
