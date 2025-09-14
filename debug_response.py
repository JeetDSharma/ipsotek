"""
Debug script to see the actual Elasticsearch response structure.
"""
from elasticsearch_client import elasticsearch_client
from models import ElasticsearchQuery
from config import config

def debug_elasticsearch_response():
    """Debug the actual Elasticsearch response structure."""
    print("=" * 60)
    print("Debugging Elasticsearch Response Structure")
    print("=" * 60)
    
    # Connect to Elasticsearch
    if not elasticsearch_client.connect():
        print("❌ Failed to connect to Elasticsearch")
        return
    
    try:
        # Make a simple search
        query = ElasticsearchQuery(
            index=config.elasticsearch.index,
            query={"match_all": {}},
            size=1  # Just get 1 document for debugging
        )
        
        # Get raw response
        search_params = {
            "index": query.index,
            "body": {
                "query": query.query,
                "size": query.size,
            }
        }
        
        response = elasticsearch_client.client.search(**search_params)
        
        print("Raw Elasticsearch response structure:")
        print("-" * 40)
        print(f"Response keys: {list(response.keys())}")
        print(f"Hits keys: {list(response['hits'].keys())}")
        
        if response['hits']['hits']:
            hit = response['hits']['hits'][0]
            print(f"\nFirst hit keys: {list(hit.keys())}")
            print(f"Hit structure:")
            for key, value in hit.items():
                if isinstance(value, dict):
                    print(f"  {key}: {type(value)} with keys {list(value.keys())}")
                else:
                    print(f"  {key}: {type(value)} = {value}")
        
        print("\n" + "=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        elasticsearch_client.disconnect()

if __name__ == "__main__":
    debug_elasticsearch_response()
