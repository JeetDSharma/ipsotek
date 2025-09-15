"""
Data models for the Elasticsearch to Firebase pipeline.
"""
from datetime import datetime
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field


class ElasticsearchDocument(BaseModel):
    """Model for Elasticsearch document structure."""
    _id: str = Field(alias="_id")
    _index: str = Field(alias="_index")
    _source: Dict[str, Any] = Field(alias="_source")
    _score: Optional[float] = Field(default=None, alias="_score")
    
    class Config:
        allow_population_by_field_name = True


class ElasticsearchHit(BaseModel):
    """Model for Elasticsearch search hit."""
    _index: str
    _id: str
    _score: Optional[float] = None
    _source: Dict[str, Any]
    _type: Optional[str] = None
    
    class Config:
        allow_population_by_field_name = True
        fields = {
            '_index': '_index',
            '_id': '_id',
            '_score': '_score',
            '_source': '_source',
            '_type': '_type'
        }


class ElasticsearchSearchResponse(BaseModel):
    """Model for Elasticsearch search response."""
    took: int
    timed_out: bool
    _shards: Dict[str, Any]
    hits: Dict[str, Any]
    
    def get_documents(self) -> List[ElasticsearchHit]:
        """Extract documents from the search response."""
        documents = []
        for hit_data in self.hits.get("hits", []):
            try:
                # Manually create ElasticsearchHit with explicit field mapping
                hit = ElasticsearchHit(
                    _index=hit_data.get("_index", ""),
                    _id=hit_data.get("_id", ""),
                    _score=hit_data.get("_score"),
                    _source=hit_data.get("_source", {}),
                    _type=hit_data.get("_type")
                )
                documents.append(hit)
            except Exception as e:
                print(f"Error creating ElasticsearchHit: {e}")
                print(f"Hit data keys: {list(hit_data.keys())}")
                raise
        return documents


class FirebaseDocument(BaseModel):
    """Model for Firebase document structure."""
    id: Optional[str] = None
    data: Dict[str, Any]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    source_index: str
    source_id: str
    
    def to_firestore_dict(self) -> Dict[str, Any]:
        """Convert to Firestore-compatible dictionary."""
        return {
            **self.data,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "source_index": self.source_index,
            "source_id": self.source_id,
        }


class PipelineStats(BaseModel):
    """Statistics for pipeline operations."""
    total_processed: int = 0
    total_successful: int = 0
    total_failed: int = 0
    last_run: Optional[datetime] = None
    last_error: Optional[str] = None
    processing_time_seconds: float = 0.0
    
    def increment_processed(self):
        """Increment processed counter."""
        self.total_processed += 1
    
    def increment_successful(self):
        """Increment successful counter."""
        self.total_successful += 1
    
    def increment_failed(self):
        """Increment failed counter."""
        self.total_failed += 1
    
    def set_error(self, error: str):
        """Set the last error message."""
        self.last_error = error
        self.last_run = datetime.utcnow()
    
    def set_success(self):
        """Mark successful run."""
        self.last_error = None
        self.last_run = datetime.utcnow()


class ElasticsearchQuery(BaseModel):
    """Model for Elasticsearch query parameters."""
    index: str
    query: Dict[str, Any] = {"match_all": {}}
    size: int = 100
    sort: Optional[List[Dict[str, Any]]] = None
    scroll: Optional[str] = None
    scroll_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to Elasticsearch query dictionary."""
        query_dict = {
            "index": self.index,
            "body": {
                "query": self.query,
                "size": self.size,
            }
        }
        
        if self.sort:
            query_dict["body"]["sort"] = self.sort
        
        if self.scroll:
            query_dict["scroll"] = self.scroll
        
        return query_dict
