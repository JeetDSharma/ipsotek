"""
Script to check Firebase data and troubleshoot the storage issue.
"""
import asyncio
from firebase_client import firebase_client
from config import config
from loguru import logger

async def check_firebase_data():
    """Check what's actually stored in Firebase."""
    print("=" * 60)
    print("Firebase Data Check")
    print("=" * 60)
    
    # Initialize Firebase
    if not firebase_client.initialize():
        print("❌ Failed to initialize Firebase")
        return
    
    try:
        # Get collection stats
        stats = await firebase_client.get_collection_stats(config.firebase.collection)
        print(f"Collection stats: {stats}")
        
        # Query for documents to see what's actually there
        print(f"\nQuerying documents from '{config.firebase.collection}' collection...")
        
        # Try to get some documents
        docs = await firebase_client.query_documents(
            collection_name=config.firebase.collection,
            field="source_index",
            operator=">=",
            value="",  # Get all documents
            limit=10
        )
        
        print(f"Found {len(docs)} documents in query")
        
        if docs:
            print("\nSample documents:")
            for i, doc in enumerate(docs[:3]):  # Show first 3
                print(f"\nDocument {i+1}:")
                print(f"  ID: {doc.get('_id', 'No ID')}")
                print(f"  Source Index: {doc.get('source_index', 'No source_index')}")
                print(f"  Source ID: {doc.get('source_id', 'No source_id')}")
                print(f"  Event Name: {doc.get('event_name', 'No event_name')}")
                print(f"  Timestamp: {doc.get('@timestamp', 'No timestamp')}")
                print(f"  Created At: {doc.get('created_at', 'No created_at')}")
        else:
            print("❌ No documents found in collection")
            
            # Try a different approach - get all documents
            print("\nTrying to get all documents...")
            try:
                # This is a more direct approach
                collection_ref = firebase_client.db.collection(config.firebase.collection)
                docs = collection_ref.limit(10).stream()
                
                doc_count = 0
                for doc in docs:
                    doc_count += 1
                    doc_data = doc.to_dict()
                    print(f"\nDocument {doc_count}:")
                    print(f"  Document ID: {doc.id}")
                    print(f"  Keys: {list(doc_data.keys())}")
                    if doc_count >= 3:  # Show only first 3
                        break
                
                print(f"\nTotal documents found: {doc_count}")
                
            except Exception as e:
                print(f"Error getting documents: {e}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 60)

async def main():
    await check_firebase_data()

if __name__ == "__main__":
    asyncio.run(main())
