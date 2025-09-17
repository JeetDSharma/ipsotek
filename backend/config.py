import os
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseSettings, Field

load_dotenv()


class ElasticsearchConfig(BaseSettings):
    """Elasticsearch connection configuration."""
    host: str = Field(default="localhost", env="ELASTICSEARCH_HOST")
    port: int = Field(default=9200, env="ELASTICSEARCH_PORT")
    username: Optional[str] = Field(default=None, env="ELASTICSEARCH_USERNAME")
    password: Optional[str] = Field(default=None, env="ELASTICSEARCH_PASSWORD")
    use_ssl: bool = Field(default=False, env="ELASTICSEARCH_USE_SSL")
    verify_certs: bool = Field(default=False, env="ELASTICSEARCH_VERIFY_CERTS")
    index: str = Field(default="logs", env="ELASTICSEARCH_INDEX")
    
    class Config:
        env_file = ".env"


class FirebaseConfig(BaseSettings):
    """Firebase configuration."""
    project_id: str = Field(env="FIREBASE_PROJECT_ID")
    private_key_id: str = Field(env="FIREBASE_PRIVATE_KEY_ID")
    private_key: str = Field(env="FIREBASE_PRIVATE_KEY")
    client_email: str = Field(env="FIREBASE_CLIENT_EMAIL")
    client_id: str = Field(env="FIREBASE_CLIENT_ID")
    auth_uri: str = Field(default="https://accounts.google.com/o/oauth2/auth", env="FIREBASE_AUTH_URI")
    token_uri: str = Field(default="https://oauth2.googleapis.com/token", env="FIREBASE_TOKEN_URI")
    collection: str = Field(default="elasticsearch_data", env="FIREBASE_COLLECTION")
    storage_bucket: str = Field(default="", env="FIREBASE_STORAGE_BUCKET")
    
    class Config:
        env_file = ".env"


class PipelineConfig(BaseSettings):
    """Pipeline operation configuration."""
    polling_interval_seconds: int = Field(default=30, env="POLLING_INTERVAL_SECONDS")
    batch_size: int = Field(default=50, env="BATCH_SIZE")
    max_retries: int = Field(default=3, env="MAX_RETRIES")
    retry_delay_seconds: int = Field(default=5, env="RETRY_DELAY_SECONDS")
    image_auth_url: str = Field(default="https://127.0.0.1/evolution/incident-response/authorize", env="IMAGE_AUTH_URL")
    image_username: str = Field(default="Admin", env="IMAGE_USERNAME")
    image_password: str = Field(default="test", env="IMAGE_PASSWORD")
    image_base_url: str = Field(default="https://127.0.0.1/evolution/incident-response/image/main", env="IMAGE_BASE_URL")
    storage_prefix: str = Field(default="events", env="STORAGE_PREFIX")
    
    class Config:
        env_file = ".env"


class TwilioConfig(BaseSettings):
    """Twilio SMS configuration."""
    account_sid: str = Field(env="TWILIO_ACCOUNT_SID")
    auth_token: str = Field(env="TWILIO_AUTH_TOKEN")
    from_phone: str = Field(env="TWILIO_FROM_PHONE")
    to_phone: str = Field(env="TWILIO_TO_PHONE")
    enabled: bool = Field(default=True, env="TWILIO_SMS_ENABLED")
    
    class Config:
        env_file = ".env"


class LoggingConfig(BaseSettings):
    """Logging configuration."""
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: str = Field(default="pipeline.log", env="LOG_FILE")
    
    class Config:
        env_file = ".env"


class Config:
    """Main configuration class that combines all config sections."""
    
    def __init__(self):
        self.elasticsearch = ElasticsearchConfig()
        self.firebase = FirebaseConfig()
        self.pipeline = PipelineConfig()
        self.twilio = TwilioConfig()
        self.logging = LoggingConfig()
    
    def get_elasticsearch_url(self) -> str:
        """Get the complete Elasticsearch URL."""
        protocol = "https" if self.elasticsearch.use_ssl else "http"
        return f"{protocol}://{self.elasticsearch.host}:{self.elasticsearch.port}"
    
    def get_firebase_credentials(self) -> dict:
        """Get Firebase credentials as a dictionary."""
        return {
            "type": "service_account",
            "project_id": self.firebase.project_id,
            "private_key_id": self.firebase.private_key_id,
            "private_key": self.firebase.private_key.replace('\\n', '\n'),
            "client_email": self.firebase.client_email,
            "client_id": self.firebase.client_id,
            "auth_uri": self.firebase.auth_uri,
            "token_uri": self.firebase.token_uri,
        }


# Global config instance
config = Config()
