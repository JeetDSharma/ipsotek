"""
Debug script to see exactly what's happening with ElasticsearchHit creation.
"""
from elasticsearch_client import elasticsearch_client
from models import ElasticsearchQuery, ElasticsearchSearchResponse
from config import config

def debug_hit_creation():
    """Debug the ElasticsearchHit creation process."""
    print("=" * 60)
    print("Debugging ElasticsearchHit Creation")
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
            size=2  # Get 2 documents for debugging
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
        
        print("Raw response hits:")
        print("-" * 40)
        for i, hit in enumerate(response['hits']['hits']):
            print(f"Hit {i+1} keys: {list(hit.keys())}")
            print(f"Hit {i+1} _index: {hit.get('_index')}")
            print(f"Hit {i+1} _id: {hit.get('_id')}")
            print(f"Hit {i+1} _source keys: {list(hit.get('_source', {}).keys())}")
            print()
        
        # Now try to create ElasticsearchSearchResponse
        print("Creating ElasticsearchSearchResponse...")
        search_response = ElasticsearchSearchResponse(**response)
        
        print("Getting documents...")
        documents = search_response.get_documents()
        
        print(f"Created {len(documents)} ElasticsearchHit objects")
        
        for i, doc in enumerate(documents):
            print(f"\nDocument {i+1}:")
            print(f"  Type: {type(doc)}")
            print(f"  Has _index: {hasattr(doc, '_index')}")
            print(f"  Has _id: {hasattr(doc, '_id')}")
            print(f"  Has _source: {hasattr(doc, '_source')}")
            
            if hasattr(doc, '_index'):
                print(f"  _index: {doc._index}")
            if hasattr(doc, '_id'):
                print(f"  _id: {doc._id}")
            if hasattr(doc, '_source'):
                source_keys = list(doc._source.keys()) if isinstance(doc._source, dict) else "Not a dict"
                print(f"  _source keys: {source_keys}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        elasticsearch_client.disconnect()

if __name__ == "__main__":
    debug_hit_creation()
