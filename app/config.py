from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    BROKER_API_URL: str = "http://api-gateway:8080"
    JWT_SECRET: str  # no default — must be set in the environment

    # ── admin / observability ──────────────────────────────────────────────────
    # Shared secret required on /internal/admin/* endpoints
    INTERNAL_SERVICE_TOKEN: str = "change-me-in-production"

    # Kafka — bot.activity.{decisions,events,lifecycle} topics
    KAFKA_BOOTSTRAP_SERVERS: str = "redpanda:9092"
    KAFKA_ENABLED: bool = True

    # Activity log throttling and retention
    DECISION_LOG_THROTTLE_SECS: int = 10
    DECISION_LOG_RETENTION_DAYS: int = 14
    EVENT_LOG_RETENTION_DAYS: int = 90
    ACTIVITY_LOG_PRUNE_HOURS: int = 6

    model_config = {"env_file": ".env"}

    @property
    def async_database_url(self) -> str:
        url = self.DATABASE_URL
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url


settings = Settings()
