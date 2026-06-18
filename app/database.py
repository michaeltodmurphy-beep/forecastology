# app/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.models import Base
import structlog

logger = structlog.get_logger(__name__)


class DatabaseManager:
    """Manages async SQLAlchemy connections."""

    def __init__(self, database_url: str):
        self.engine = create_async_engine(
            database_url,
            pool_size=5,
            max_overflow=10,
            echo=False,
        )
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def initialize(self):
        """Create all tables if they don't exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("database.initialized")

    async def get_session(self) -> AsyncSession:
        return self.session_factory()

    async def dispose(self):
        await self.engine.dispose()
        logger.info("database.disposed")
