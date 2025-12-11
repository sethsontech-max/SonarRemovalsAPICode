'''
def main():
    # Configuration
    GRAPHQL_ENDPOINT = os.getenv("SONAR_GRAPHQL_ENDPOINT", "https://your-sonar-instance.com/graphql")
    API_KEY = os.getenv("SONAR_API_KEY")  # Optional, set if needed
    
    # Define your inventory model IDs
    inventory_model_ids = [3, 4, 5, 6, 7]
    
    # Define deployment types to create
    deployment_types = [
        "Active",
        "Inactive",
        "Maintenance",
        "Retired"
    ]
    
    # Initialize client
    client = SonarGraphQLClient(GRAPHQL_ENDPOINT, API_KEY)
    
    try:
        # Create deployment types
        results = create_deployment_types_bulk(
            client,
            inventory_model_ids,
            deployment_types
        )
        
        # Print results
        print("\n✓ Success! Created deployment types:")
        print(json.dumps(results, indent=2))
        
        # Summary
        total_created = len(results)
        print(f"\nTotal deployment types created: {total_created}")
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
    
    '''


class SonarGraphQLClient:
    def __init__(self, endpoint: str, api_key: str = None):
        """
        Initialize the GraphQL client
        
        Args:
            endpoint: GraphQL API endpoint URL
            api_key: Optional API key for authentication
        """
        self.endpoint = endpoint
        self.headers = {
            "Content-Type": "application/json",
        }
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"
    
    def execute_mutation(self, mutation: str, variables: dict = None):
        """Execute a GraphQL mutation"""
        payload = {
            "query": mutation,
            "variables": variables or {}
        }
        
        response = requests.post(
            self.endpoint,
            headers=self.headers,
            json=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"Query failed with status {response.status_code}: {response.text}")
        
        result = response.json()
        
        if "errors" in result:
            raise Exception(f"GraphQL errors: {json.dumps(result['errors'], indent=2)}")
        
        return result["data"]


def create_deployment_types_bulk(
    client: SonarGraphQLClient,
    inventory_model_ids: List[int],
    deployment_types: List[str]
):
    """
    Create multiple deployment types for multiple inventory model IDs
    
    Args:
        client: SonarGraphQLClient instance
        inventory_model_ids: List of inventory model IDs
        deployment_types: List of deployment type names to create
    
    Returns:
        Dictionary with results
    """
    # Build the mutation with aliases
    mutation_parts = ["mutation CreateDeploymentTypes {"]
    
    for model_id in inventory_model_ids:
        for dtype in deployment_types:
            # Create a safe alias (replace spaces/special chars with underscores)
            safe_name = dtype.replace(" ", "_").replace("-", "_").lower()
            alias = f"id{model_id}_{safe_name}"
            
            mutation_parts.append(f"""
  {alias}: createDeploymentType(input: {{
    inventory_model_id: {model_id}
    name: "{dtype}"
  }}) {{
    id
  }}""")
    
    mutation_parts.append("\n}")
    mutation = "\n".join(mutation_parts)
    
    print("Executing mutation...")
    print(f"Creating {len(deployment_types)} deployment types for {len(inventory_model_ids)} models")
    print(f"Total operations: {len(deployment_types) * len(inventory_model_ids)}\n")
    
    # Execute the mutation
    result = client.execute_mutation(mutation)
    
    return result
