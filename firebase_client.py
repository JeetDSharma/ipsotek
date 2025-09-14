"""
Firebase client for data storage.
"""
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.exceptions import FirebaseError
from loguru import logger

from config import config
from models import FirebaseDocument, ElasticsearchHit


class FirebaseClient:
    """Client for interacting with Firebase Firestore."""
    
    def __init__(self):
        self.app: Optional[firebase_admin.App] = None
        self.db: Optional[firestore.Client] = None
        self.is_initialized = False
    
    def initialize(self) -> bool:
        """Initialize Firebase Admin SDK."""
        try:
            # Check if Firebase is already initialized
            if firebase_admin._apps:
                self.app = firebase_admin.get_app()
            else:
                # Create credentials from config
                cred_dict = config.get_firebase_credentials()
                cred = credentials.Certificate(cred_dict)
                
                # Initialize Firebase Admin
                self.app = firebase_admin.initialize_app(cred)
            
            # Get Firestore client
            self.db = firestore.client()
            self.is_initialized = True
            
            logger.info(f"Firebase initialized for project: {config.firebase.project_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {e}")
            self.is_initialized = False
            return False
    
    def _convert_elasticsearch_hit_to_firebase_document(
        self,
        hit: ElasticsearchHit,
        collection_name: str
    ) -> FirebaseDocument:
        """Convert Elasticsearch hit to Firebase document."""
        # Handle cases where _index might not be available
        index_name = getattr(hit, '_index', 'unknown_index')
        doc_id = getattr(hit, '_id', 'unknown_id')
        
        return FirebaseDocument(
            id=f"{index_name}_{doc_id}",  # Create unique ID
            data=hit._source,
            source_index=index_name,
            source_id=doc_id
        )
    
    def _prepare_document_for_firestore(self, document: FirebaseDocument) -> Dict[str, Any]:
        """Prepare document for Firestore storage."""
        firestore_data = document.to_firestore_dict()
        
        # Convert any non-serializable objects to strings
        for key, value in firestore_data.items():
            if isinstance(value, datetime):
                firestore_data[key] = value
            elif not self._is_firestore_compatible(value):
                firestore_data[key] = json.dumps(value, default=str)
        
        return firestore_data
    
    def _is_firestore_compatible(self, value: Any) -> bool:
        """Check if a value is compatible with Firestore."""
        compatible_types = (
            str, int, float, bool, datetime, 
            type(None), list, dict
        )
        
        if isinstance(value, compatible_types):
            if isinstance(value, (list, dict)):
                # Check nested values
                if isinstance(value, list):
                    return all(self._is_firestore_compatible(item) for item in value)
                else:  # dict
                    return all(self._is_firestore_compatible(v) for v in value.values())
            return True
        
        return False
    
    async def store_document(
        self,
        document: FirebaseDocument,
        collection_name: str
    ) -> bool:
        """Store a single document in Firestore."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            firestore_data = self._prepare_document_for_firestore(document)
            
            # Use document ID if provided, otherwise let Firestore generate one
            doc_ref = self.db.collection(collection_name)
            if document.id:
                doc_ref = doc_ref.document(document.id)
            
            doc_ref.set(firestore_data)
            
            logger.debug(f"Stored document {document.id} in collection {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store document {document.id}: {e}")
            return False
    
    async def store_documents_batch(
        self,
        documents: List[FirebaseDocument],
        collection_name: str
    ) -> int:
        """Store multiple documents in Firestore using batch write."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            if not documents:
                return 0
            
            # Firestore batch limit is 500 operations
            batch_size = 500
            successful_stores = 0
            
            for i in range(0, len(documents), batch_size):
                batch = self.db.batch()
                batch_documents = documents[i:i + batch_size]
                
                for document in batch_documents:
                    firestore_data = self._prepare_document_for_firestore(document)
                    
                    doc_ref = self.db.collection(collection_name)
                    if document.id:
                        doc_ref = doc_ref.document(document.id)
                    
                    batch.set(doc_ref, firestore_data)
                
                # Commit the batch
                batch.commit()
                successful_stores += len(batch_documents)
                
                logger.debug(f"Stored batch of {len(batch_documents)} documents")
            
            logger.info(f"Successfully stored {successful_stores} documents in {collection_name}")
            return successful_stores
            
        except Exception as e:
            logger.error(f"Failed to store documents batch: {e}")
            return 0
    
    async def store_elasticsearch_hits(
        self,
        hits: List[ElasticsearchHit],
        collection_name: str
    ) -> int:
        """Convert and store Elasticsearch hits in Firestore."""
        try:
            if not hits:
                return 0
            
            # Convert hits to Firebase documents
            firebase_documents = []
            for hit in hits:
                document = self._convert_elasticsearch_hit_to_firebase_document(hit, collection_name)
                firebase_documents.append(document)
            
            # Store documents
            return await self.store_documents_batch(firebase_documents, collection_name)
            
        except Exception as e:
            logger.error(f"Failed to store Elasticsearch hits: {e}")
            return 0
    
    async def get_document(
        self,
        document_id: str,
        collection_name: str
    ) -> Optional[Dict[str, Any]]:
        """Retrieve a document from Firestore."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc = doc_ref.get()
            
            if doc.exists:
                return doc.to_dict()
            else:
                logger.warning(f"Document {document_id} not found in {collection_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get document {document_id}: {e}")
            return None
    
    async def query_documents(
        self,
        collection_name: str,
        field: str,
        operator: str,
        value: Any,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query documents from Firestore."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            query = self.db.collection(collection_name)
            query = query.where(field, operator, value)
            query = query.limit(limit)
            
            docs = query.stream()
            results = []
            
            for doc in docs:
                doc_data = doc.to_dict()
                doc_data['_id'] = doc.id
                results.append(doc_data)
            
            logger.debug(f"Retrieved {len(results)} documents from {collection_name}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to query documents: {e}")
            return []
    
    async def delete_document(
        self,
        document_id: str,
        collection_name: str
    ) -> bool:
        """Delete a document from Firestore."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc_ref.delete()
            
            logger.debug(f"Deleted document {document_id} from {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete document {document_id}: {e}")
            return False
    
    async def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get basic statistics about a collection."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            # Get a sample of documents to estimate collection size
            docs = self.db.collection(collection_name).limit(1000).stream()
            doc_count = sum(1 for _ in docs)
            
            return {
                "collection_name": collection_name,
                "estimated_document_count": doc_count,
                "last_checked": datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to get collection stats: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """Test the Firebase connection."""
        try:
            if not self.is_initialized or not self.db:
                return False
            
            # Try to access a collection (this will fail if not authenticated)
            collections = self.db.collections()
            return True
            
        except Exception as e:
            logger.error(f"Firebase connection test failed: {e}")
            return False


# Global Firebase client instance
firebase_client = FirebaseClient()
