from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    BROKER_API_URL: str = "http://api-gateway:8080"
    JWT_SECRET: str = "changeme"

    model_config = {"env_file": ".env"}

    @property
    def async_database_url(self) -> str:
        url = self.DATABASE_URL
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url


settings = Settings()
