"""
GraphQL Bulk Deployment Type Creator for Sonar
Creates multiple deployment types across a range of inventory model IDs

SETUP FROM SCRATCH IN VISUAL STUDIO CODE:

1. Install Python (if not already installed):
   - Download from https://www.python.org/downloads/
   - During installation, check "Add Python to PATH"
   - Verify: Open terminal and run: python --version

2. Install Visual Studio Code:
   - Download from https://code.visualstudio.com/
   - Install the Python extension (Ctrl+Shift+X, search "Python", install by Microsoft)

3. Create Project Folder:
   - Create a new folder for your project (e.g., "sonar-graphql")
   - Open VS Code: File > Open Folder > Select your project folder

4. Create Virtual Environment (Recommended):
   - Open terminal in VS Code (Terminal > New Terminal or Ctrl+`)
   - Run: python -m venv venv
   - Activate it:
     Windows: venv\Scripts\activate
     Mac/Linux: source venv/bin/activate
   - You should see (venv) in your terminal prompt

5. Install Required Packages:
   pip install requests python-dotenv

6. Create Files:
   - Create this script file: create_deployment_types.py
   - Create a .env file (File > New File > Save as ".env")

7. Configure .env file:
   SONAR_GRAPHQL_ENDPOINT=https://your-sonar-instance.com/graphql
   SONAR_API_KEY=your_api_key_here

8. Run the Script:
   - In VS Code terminal: python create_deployment_types.py
   - Or press F5 to debug

Required packages:
pip install requests python-dotenv
"""

import requests
import json
from typing import List
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("secrets.env")



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

def create_query_deployment_types(
    inventory_model_ids: List[int],
    deployment_types: List[str]
) -> List[str]:
    """
    Create multiple deployment type mutation strings for multiple inventory model IDs.

    Each returned string is a complete GraphQL mutation that contains at most
    `max_per_chunk` createDeploymentType operations (default 99). This function
    returns a list of mutation strings ready to be POSTed to the GraphQL API.

    Args:
        inventory_model_ids: List of inventory model IDs
        deployment_types: List of deployment type names to create

    Returns:
        List[str]: list of mutation strings (each <= `max_per_chunk` operations)
    """
    # Build individual operation strings
    operations: List[str] = []
    for model_id in inventory_model_ids:
        for dtype in deployment_types:
            # Create a safe alias (replace spaces/special chars with underscores)
            safe_name = dtype.replace(" ", "_").replace("-", "_").lower()
            alias = f"id{model_id}_{safe_name}"

            op = f"""
  {alias}: createDeploymentType(input: {{
    inventory_model_id: {model_id}
    name: "{dtype}"
  }}) {{
    id
  }}"""

            operations.append(op)

    # Chunk operations into groups no larger than max_per_chunk
    max_per_chunk = 99
    chunks: List[List[str]] = [operations[i:i+max_per_chunk] for i in range(0, len(operations), max_per_chunk)]

    mutation_strings: List[str] = []
    for chunk in chunks:
        parts = ["mutation CreateDeploymentTypes {"] + chunk + ["\n}"]
        mutation = "\n".join(parts)
        mutation_strings.append(mutation)

    print("Preparing mutation(s)...")
    print(f"Creating {len(deployment_types)} deployment types for {len(inventory_model_ids)} models")
    total_ops = len(operations)
    print(f"Total operations: {total_ops}")
    print(f"Split into {len(mutation_strings)} mutation(s) with up to {max_per_chunk} operations each\n")



    return mutation_strings

def build_api_request_and_execute(query: str):
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
    payload = {
        "query": query
    }

    print("\nSending mutation...\n")
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
    print("Mutation executed successfully!\n")
    return result

def get_Inventory_Model_IDs_query():

    query = """ query getInventoryModels {
  inventory_models{
    entities{
      id
    }
  }
}"""
    return query



def main():

    modelIDs_query = get_Inventory_Model_IDs_query()
    # print("modelIDs_query:", modelIDs_query)

    result = build_api_request_and_execute(modelIDs_query)
    if result is not None:
        inventory_model_ids = [entity['id'] for entity in result['data']['inventory_models']['entities']]
        print(f"Retrieved Inventory Model IDs: {inventory_model_ids}")



    #inventory_model_ids = ['4'] 
    deployment_types = ["Active - Customer", "Active - Infrastructure", "Inactive Reserve", "Maintenance", "Lost", "Awaiting Recovery"]

    mutations = create_query_deployment_types(inventory_model_ids, deployment_types)

    print(f"Generated {len(mutations)} mutation(s).")

    all_results = []
    for idx, mut in enumerate(mutations, start=1):
        print(f"\n--- Executing mutation {idx}/{len(mutations)} ---")
        res = build_api_request_and_execute(mut)
        all_results.append(res)
        if res is not None:
            print(f"Mutation {idx} executed successfully!")
        else:
            print(f"Mutation {idx} failed.")

    # Pretty-print aggregated JSON results
    print("Result(s):")
    print(json.dumps(all_results, indent=2, ensure_ascii=False))





main()