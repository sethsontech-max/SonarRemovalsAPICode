"""
Simple GraphQL POST example using endpoint and API key from `secrets.env`.

- Create a `secrets.env` file in the same folder with these keys:

  SONAR_GRAPHQL_ENDPOINT=https://your-sonar-instance.com/graphql
  SONAR_API_KEY=your_api_key_here

- Install dependencies: `pip install requests python-dotenv`
- Run: `python graphql_post_basic.py`

This script sends a small query and prints the JSON response.
"""

import os
import json
import requests
from dotenv import load_dotenv

# Load environment variables from secrets.env (file name is explicit)
load_dotenv("secrets.env")

ENDPOINT = os.getenv("SONAR_GRAPHQL_ENDPOINT")
API_KEY = os.getenv("SONAR_API_KEY")

if not ENDPOINT:
    print("ERROR: SONAR_GRAPHQL_ENDPOINT not set in secrets.env")
    raise SystemExit(1)

headers = {"Content-Type": "application/json"}
if API_KEY:
    headers["Authorization"] = f"Bearer {API_KEY}"

# Example GraphQL query (adjust to your schema)
QUERY = """
query InventoryModels {
  inventory_models {
    entities {
      id
      model_name
      name
    }
  }
}
"""

PAYLOAD = {"query": QUERY}


def post_graphql(endpoint: str, payload: dict, headers: dict, timeout: int = 10) -> dict:
    """Send a GraphQL POST and return the parsed JSON result.

    Raises on network errors or invalid JSON.
    """
    resp = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    try:
        print(f"Posting GraphQL to: {ENDPOINT}")
        result = post_graphql(ENDPOINT, PAYLOAD, headers)
        print(json.dumps(result, indent=2))
    except requests.exceptions.HTTPError as he:
        print(f"HTTP error: {he}")
        try:
            print("Response body:")
            print(resp.text)
        except Exception:
            pass
    except requests.exceptions.RequestException as re:
        print(f"Request failed: {re}")
    except json.JSONDecodeError:
        print("Failed to decode JSON response")
    except Exception as e:
        print(f"Unexpected error: {e}")
