"""
Script to capture and analyze Elasticsearch response for model compatibility.
"""
import json
from datetime import datetime
from elasticsearch_client import elasticsearch_client
from models import ElasticsearchQuery, ElasticsearchSearchResponse, ElasticsearchHit
from config import config

def capture_and_analyze_response():
    """Capture Elasticsearch response and analyze model compatibility."""
    print("=" * 60)
    print("Elasticsearch Response Capture & Analysis")
    print("=" * 60)
    
    # Connect to Elasticsearch
    if not elasticsearch_client.connect():
        print("❌ Failed to connect to Elasticsearch")
        return
    
    try:
        # Make a search to get sample data
        query = ElasticsearchQuery(
            index=config.elasticsearch.index,
            query={"match_all": {}},
            size=3  # Get 3 documents for analysis
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
        
        # Save raw response to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"elasticsearch_response_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(response, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"✅ Raw response saved to: {filename}")
        
        # Analyze the response structure
        print("\n" + "=" * 60)
        print("RESPONSE STRUCTURE ANALYSIS")
        print("=" * 60)
        
        print(f"Top-level keys: {list(response.keys())}")
        print(f"Hits keys: {list(response['hits'].keys())}")
        print(f"Number of documents: {len(response['hits']['hits'])}")
        
        # Analyze each document
        for i, hit in enumerate(response['hits']['hits']):
            print(f"\n--- DOCUMENT {i+1} ---")
            print(f"Hit keys: {list(hit.keys())}")
            print(f"_index: {hit.get('_index')}")
            print(f"_id: {hit.get('_id')}")
            print(f"_type: {hit.get('_type')}")
            print(f"_score: {hit.get('_score')}")
            
            source = hit.get('_source', {})
            print(f"_source keys count: {len(source)}")
            print(f"Sample _source keys: {list(source.keys())[:10]}...")
            
            # Check for specific fields we care about
            important_fields = ['@timestamp', 'event_name', 'camera_name', 'object_id', 'event_time']
            print(f"Important fields present:")
            for field in important_fields:
                if field in source:
                    print(f"  ✅ {field}: {source[field]}")
                else:
                    print(f"  ❌ {field}: MISSING")
        
        # Test model creation
        print("\n" + "=" * 60)
        print("MODEL CREATION TEST")
        print("=" * 60)
        
        try:
            # Test ElasticsearchSearchResponse creation
            print("Testing ElasticsearchSearchResponse creation...")
            search_response = ElasticsearchSearchResponse(**response)
            print("✅ ElasticsearchSearchResponse created successfully")
            
            # Test ElasticsearchHit creation
            print("Testing ElasticsearchHit creation...")
            documents = search_response.get_documents()
            print(f"✅ Created {len(documents)} ElasticsearchHit objects")
            
            # Analyze each created document
            for i, doc in enumerate(documents):
                print(f"\n--- CREATED DOCUMENT {i+1} ---")
                print(f"Type: {type(doc)}")
                print(f"Has _index: {hasattr(doc, '_index')}")
                print(f"Has _id: {hasattr(doc, '_id')}")
                print(f"Has _source: {hasattr(doc, '_source')}")
                
                if hasattr(doc, '_index'):
                    print(f"_index value: {doc._index}")
                if hasattr(doc, '_id'):
                    print(f"_id value: {doc._id}")
                if hasattr(doc, '_source'):
                    source = doc._source
                    if isinstance(source, dict):
                        print(f"_source type: dict with {len(source)} keys")
                        print(f"Sample _source keys: {list(source.keys())[:5]}")
                    else:
                        print(f"_source type: {type(source)}")
                        print(f"_source value: {source}")
                
        except Exception as e:
            print(f"❌ Model creation failed: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n✅ Analysis complete. Check {filename} for full response.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        elasticsearch_client.disconnect()

if __name__ == "__main__":
    capture_and_analyze_response()
