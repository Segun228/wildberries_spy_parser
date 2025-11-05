from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "API Gateway"
    debug: bool = False
    jwt_secret: str = "your-secret-key"
    rate_limit_requests: int = 100
    rate_limit_window: int = 3600
    
    class Config:
        env_file = ".env"

settings = Settings()