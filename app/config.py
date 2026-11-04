from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "taskdb"
    redis_url: str = "redis://localhost:6379"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "tasks.events"
    kafka_consumer_group: str = "task-dependency-service"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "DEBUG"
    # Set to "json" in production for structured log ingestion (ELK, CloudWatch, etc.)
    log_format: Literal["text", "json"] = "text"


settings = Settings()
