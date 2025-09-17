"""
Simplified Firebase client that works with raw dictionaries.
"""
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.exceptions import FirebaseError
from loguru import logger

from config import config


class SimpleFirebaseClient:
    """Simplified Firebase client that works with raw dictionaries."""
    
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
    
    def _prepare_document_for_firestore(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare document for Firestore storage."""
        # Add metadata
        doc_data["created_at"] = datetime.utcnow()
        doc_data["updated_at"] = datetime.utcnow()
        
        # Convert any non-serializable objects to strings
        for key, value in doc_data.items():
            if isinstance(value, datetime):
                continue  # Firestore handles datetime objects
            elif not self._is_firestore_compatible(value):
                doc_data[key] = json.dumps(value, default=str)
        
        return doc_data
    
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
    
    def store_documents_batch(
        self,
        documents: List[Dict[str, Any]],
        collection_name: str
    ) -> int:
        """Store multiple documents in Firestore using batch write."""
        try:
            if not self.is_initialized or not self.db:
                raise Exception("Firebase client not initialized")
            
            if not documents:
                return 0
            
            # Use smaller batch size to avoid timeouts
            batch_size = 50  # Much smaller batches for reliability
            successful_stores = 0
            total_documents = len(documents)
            
            logger.info(f"Storing {total_documents} documents in batches of {batch_size}")
            
            for i in range(0, total_documents, batch_size):
                batch_documents = documents[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_documents + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_documents)} documents)")
                
                try:
                    # Create batch
                    batch = self.db.batch()
                    
                    for doc in batch_documents:
                        # Create unique document ID
                        doc_id = f"{doc.get('_index', 'unknown')}_{doc.get('_id', 'unknown')}"
                        
                        # Prepare document data
                        firestore_data = self._prepare_document_for_firestore(doc.get('_source', {}))
                        
                        # Add source metadata
                        firestore_data["source_index"] = doc.get('_index', 'unknown')
                        firestore_data["source_id"] = doc.get('_id', 'unknown')
                        
                        # Set document in batch
                        doc_ref = self.db.collection(collection_name).document(doc_id)
                        batch.set(doc_ref, firestore_data)
                    
                    # Commit the batch
                    batch.commit()
                    successful_stores += len(batch_documents)
                    
                    logger.info(f"✅ Successfully stored batch {batch_num}/{total_batches} ({len(batch_documents)} documents)")
                    
                except Exception as batch_error:
                    logger.error(f"❌ Failed to store batch {batch_num}/{total_batches}: {batch_error}")
                    # Continue with next batch instead of failing completely
                    continue
                
                # Small delay between batches to avoid overwhelming Firebase
                if i + batch_size < total_documents:
                    time.sleep(0.2)  # 200ms delay between batches
            
            logger.info(f"Successfully stored {successful_stores}/{total_documents} documents in {collection_name}")
            return successful_stores
            
        except Exception as e:
            logger.error(f"Failed to store documents batch: {e}")
            return 0
    
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
firebase_client = SimpleFirebaseClient()
