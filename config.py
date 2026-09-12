import os
from pathlib import Path
from dotenv import load_dotenv

# Programmatically resolve the exact location of the .env file in the workspace root
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

class AppSettings:
    """Production configuration engine for AFAOS Core Modules."""
    
    # PostgreSQL Credentials and Routing Parameters
    DB_USER: str = os.getenv("POSTGRES_USER", "audit_admin")
    DB_PASS: str = os.getenv("POSTGRES_PASSWORD", "secure_audit_pass")
    DB_HOST: str = os.getenv("POSTGRES_HOST", "127.0.0.1")
    DB_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME: str = os.getenv("POSTGRES_DB", "financial_ledger")
    
    @property
    def DATABASE_URL(self) -> str:
        """Generates the async dialect connection string required by SQLAlchemy/asyncpg."""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # Qdrant Standalone Vector Engine Parameters
    QDRANT_HOST: str = os.getenv("QDRANT_HOST", "127.0.0.1")
    QDRANT_PORT: int = int(os.getenv("QDRANT_PORT", 6333))

# Initialize a single, globally immutable settings token for application-wide imports
settings = AppSettings()

if __name__ == "__main__":
    # Internal Sanity Check Validation Script
    print("\n🚀 [AFAOS CONFIG] Initializing database validation checks...")
    print(f"🔗 Target Async Postgres DB Link: {settings.DATABASE_URL}")
    print(f"🎯 Target Qdrant Vector Engine   : {settings.QDRANT_HOST}:{settings.QDRANT_PORT}\n")
