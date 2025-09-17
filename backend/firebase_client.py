import json
import time
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Union, Tuple
import firebase_admin
from firebase_admin import credentials, firestore, storage
from firebase_admin.exceptions import FirebaseError
from loguru import logger
from PIL import Image, ImageDraw
import io

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
            # Initialize default storage bucket if configured
            if config.firebase.storage_bucket:
                storage.bucket(config.firebase.storage_bucket)
            self.is_initialized = True
            
            logger.info(f"Firebase initialized for project: {config.firebase.project_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {e}")
            self.is_initialized = False
            return False

    def parse_bbox_coordinates(self, bbox_string: str) -> Optional[Tuple[float, float, float, float]]:
        """
        Parse BBOX coordinates from string like 'BBOX (359.8,452.8,672.8,669.0)'
        Returns (x1, y1, x2, y2) or None if parsing fails
        """
        try:
            # Extract numbers from the BBOX string
            pattern = r'BBOX\s*\(([^)]+)\)'
            match = re.search(pattern, bbox_string.strip())
            if not match:
                logger.warning(f"Could not parse BBOX string: {bbox_string}")
                return None
            
            coords_str = match.group(1)
            coords = [float(x.strip()) for x in coords_str.split(',')]
            
            if len(coords) != 4:
                logger.warning(f"Expected 4 coordinates, got {len(coords)}: {bbox_string}")
                return None
            
            x1, y1, x2, y2 = coords
            logger.info(f"Parsed BBOX coordinates: ({x1}, {y1}, {x2}, {y2})")
            return (x1, y1, x2, y2)
            
        except Exception as e:
            logger.error(f"Error parsing BBOX coordinates '{bbox_string}': {e}")
            return None

    def draw_rectangle_on_image(
        self, 
        image_bytes: bytes, 
        bbox_coords: Tuple[float, float, float, float],
        rectangle_color: str = "red",
        rectangle_width: int = 3
    ) -> bytes:
        """
        Draw a red rectangle on the image at the specified BBOX coordinates.
        Returns the modified image as bytes.
        """
        try:
            # Open image from bytes
            image = Image.open(io.BytesIO(image_bytes))
            
            # Convert to RGB if necessary (for PNG with transparency)
            if image.mode in ('RGBA', 'LA', 'P'):
                # Create a white background
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                image = background
            elif image.mode != 'RGB':
                image = image.convert('RGB')
            
            # Get image dimensions
            img_width, img_height = image.size
            # logger.info(f"Image dimensions: {img_width}x{img_height}")
            
            # Parse BBOX coordinates
            x1, y1, x2, y2 = bbox_coords
            
            # Ensure coordinates are within image bounds
            x1 = max(0, min(x1, img_width))
            y1 = max(0, min(y1, img_height))
            x2 = max(0, min(x2, img_width))
            y2 = max(0, min(y2, img_height))
            
            # Ensure x2 > x1 and y2 > y1
            if x2 <= x1:
                x2 = x1 + 10  # Minimum width
            if y2 <= y1:
                y2 = y1 + 10  # Minimum height
            
            logger.info(f"Drawing rectangle at: ({x1}, {y1}) to ({x2}, {y2})")
            
            # Create drawing context
            draw = ImageDraw.Draw(image)
            
            # Draw rectangle outline
            draw.rectangle(
                [x1, y1, x2, y2],
                outline=rectangle_color,
                width=rectangle_width
            )
            
            # Convert back to bytes
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=95)
            return output.getvalue()
            
        except Exception as e:
            logger.error(f"Error drawing rectangle on image: {e}")
            return image_bytes  # Return original image if processing fails

    def process_image_with_bbox(
        self,
        image_bytes: bytes,
        image_position: Optional[str],
        destination_path: str,
        content_type: str = "image/jpeg"
    ) -> dict:
        """
        Process image with BBOX rectangle and upload to Firebase Storage.
        If image_position contains BBOX coordinates, draw a red rectangle.
        """
        try:
            processed_bytes = image_bytes
            
            # Process image with BBOX if position data is available
            if image_position and "BBOX" in image_position.upper():
                logger.info(f"Processing image with BBOX: {image_position}")
                bbox_coords = self.parse_bbox_coordinates(image_position)
                
                if bbox_coords:
                    processed_bytes = self.draw_rectangle_on_image(image_bytes, bbox_coords)
                    logger.info("Successfully drew rectangle on image")
                else:
                    logger.warning("Could not parse BBOX coordinates, uploading original image")
            else:
                logger.info("No BBOX coordinates found, uploading original image")
            
            # Upload the processed image
            return self.upload_image_bytes(
                processed_bytes, 
                destination_path, 
                content_type
            )
            
        except Exception as e:
            logger.error(f"Error processing image with BBOX: {e}")
            # Fallback to original upload method
            return self.upload_image_bytes(image_bytes, destination_path, content_type)

    def upload_image_bytes(
        self,
        data: bytes,
        destination_path: str,
        content_type: str = "image/jpeg",
        make_public: bool = False
    ) -> dict:
        """Upload image bytes to Firebase Storage and return paths/URLs."""
        try:
            if not self.is_initialized:
                raise Exception("Firebase client not initialized")
            if not config.firebase.storage_bucket:
                raise Exception("Firebase storage bucket not configured")

            bucket = storage.bucket(config.firebase.storage_bucket)
            import uuid as _uuid
            blob = bucket.blob(destination_path)
            # Create a download token (used by console and REST API)
            download_token = _uuid.uuid4().hex
            blob.metadata = {"firebaseStorageDownloadTokens": download_token}
            blob.upload_from_string(data, content_type=content_type)


            logger.info(f"Uploaded image to storage: gs://{bucket.name}/{destination_path}")

            # Build REST API URLs (work for public files; for private, require auth)
            from urllib.parse import quote as _urlquote
            encoded_path = _urlquote(destination_path, safe="")
            base_url = f"https://firebasestorage.googleapis.com/v0/b/{bucket.name}/o/{encoded_path}"
            media_url = f"{base_url}?alt=media"
            media_url_with_token = f"{base_url}?alt=media&token={download_token}"

            return {
                "bucket": bucket.name,
                "path": destination_path,
                "media_url": media_url,
                "media_url_with_token": media_url_with_token,
            }
        except Exception as e:
            logger.error(f"Failed to upload image to storage: {e}")
            return {}
    
    def _prepare_document_for_firestore(self, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare document for Firestore storage."""
        # Add metadata
        doc_data["created_at"] = datetime.utcnow()
        doc_data["updated_at"] = datetime.utcnow()
        doc_data["status"] = "Pending"

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
            batch_size = config.pipeline.batch_size
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
