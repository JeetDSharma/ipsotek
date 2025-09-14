"""
Simple Elasticsearch connection test script.
"""
import json
from elasticsearch_client import elasticsearch_client
from config import config

def test_elasticsearch():
    """Test Elasticsearch connection and basic operations."""
    print("=" * 60)
    print("Elasticsearch Connection Test")
    print("=" * 60)
    
    # Test basic connection
    print("1. Testing basic connection...")
    if elasticsearch_client.connect():
        print("✅ Connected successfully!")
    else:
        print("❌ Connection failed!")
        return
    
    # Test ping
    print("\n2. Testing ping...")
    if elasticsearch_client.test_connection():
        print("✅ Ping successful!")
    else:
        print("❌ Ping failed!")
    
    # Test cluster info (simpler than health check)
    print("\n3. Testing cluster info...")
    try:
        if elasticsearch_client.client:
            info = elasticsearch_client.client.info()
            print(f"✅ Cluster info retrieved!")
            print(f"   Elasticsearch version: {info['version']['number']}")
            print(f"   Cluster name: {info['cluster_name']}")
    except Exception as e:
        print(f"❌ Cluster info failed: {e}")
    
    # Test index existence
    print(f"\n4. Testing index '{config.elasticsearch.index}'...")
    try:
        if elasticsearch_client.client:
            exists = elasticsearch_client.client.indices.exists(index=config.elasticsearch.index)
            if exists:
                print(f"✅ Index '{config.elasticsearch.index}' exists!")
                
                # Get index stats
                stats = elasticsearch_client.client.indices.stats(index=config.elasticsearch.index)
                doc_count = stats['indices'][config.elasticsearch.index]['total']['docs']['count']
                print(f"   Document count: {doc_count}")
            else:
                print(f"⚠️  Index '{config.elasticsearch.index}' does not exist!")
    except Exception as e:
        print(f"❌ Index check failed: {e}")
    
    # Test simple search
    print(f"\n5. Testing simple search on '{config.elasticsearch.index}'...")
    try:
        if elasticsearch_client.client:
            response = elasticsearch_client.client.search(
                index=config.elasticsearch.index,
                query={"match_all": {}},
                size=1
            )
            hits = response['hits']['total']
            print(f"✅ Search successful! Total documents: {hits}")
    except Exception as e:
        print(f"❌ Search failed: {e}")
    
    # Cleanup
    elasticsearch_client.disconnect()
    print("\n" + "=" * 60)
    print("Test completed!")

if __name__ == "__main__":
    test_elasticsearch()
