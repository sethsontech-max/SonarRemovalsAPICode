"""
Example: GraphQL query with JSON variables (paginator, search).

This demonstrates how to structure and POST a GraphQL query that uses
variables for complex nested JSON objects like paginator and search filters.
"""

import requests
import json
from dotenv import load_dotenv
import os

load_dotenv("secrets.env")

ENDPOINT = os.getenv("SONAR_GRAPHQL_ENDPOINT")
API_KEY = os.getenv("SONAR_API_KEY")

# ============================================================================
# GraphQL Query (with variable placeholders)
# ============================================================================
QUERY = """
query getInventoryModelIDs($paginator: Paginator, $search: Search) {
  inventory_items(paginator: $paginator, search: [$search]) {
    entities {
      id
      inventoryitemable_type
      deployment_type_id
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
    },
    "search": {
        "string_fields": [
            {
                "attribute": "inventoryitemable_type",
                "search_value": "NetworkSite",
                "match": True,
                "partial_matching": True
            }
        ]
    }
}

# ============================================================================
# Build the GraphQL payload
# ============================================================================
PAYLOAD = {
    "query": QUERY,
    "variables": VARIABLES
}

# ============================================================================
# Setup headers and POST
# ============================================================================
headers = {
    "Content-Type": "application/json",
}
if API_KEY:
    headers["Authorization"] = f"Bearer {API_KEY}"

print("=" * 70)
print("GraphQL Query with Variables Example")
print("=" * 70)

print("\n📋 Query:")
print(QUERY)

print("\n🔧 Variables (as JSON):")
print(json.dumps(VARIABLES, indent=2))

print("\n📦 Full Payload being sent:")
print(json.dumps(PAYLOAD, indent=2))

print("\n" + "=" * 70)
print("Posting to GraphQL endpoint...")
print("=" * 70 + "\n")

try:
    response = requests.post(
        ENDPOINT,
        headers=headers,
        json=PAYLOAD,  # requests will serialize this to JSON automatically
        timeout=10
    )

    print(f"HTTP Status: {response.status_code}")

    result = response.json()

    if "errors" in result:
        print("\n❌ GraphQL Errors:")
        print(json.dumps(result["errors"], indent=2))
    else:
        print("\n✅ Success!")
        print("\nResponse Data:")
        print(json.dumps(result["data"], indent=2))

except requests.exceptions.RequestException as e:
    print(f"❌ Request failed: {e}")
except Exception as e:
    print(f"❌ Error: {e}")
