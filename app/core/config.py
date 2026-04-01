from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    ENV: str

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int

    DATABASE_URL: str
    REDIS_URL: str

    TED_API_KEY: str
    TED_BASE_URL: str = "https://api.ted.europa.eu/v3"

    class Config:
        env_file = ".env.dev"

settings = Settings()