from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
import os

# Станет так: если переменная окружения задана (в Docker), возьмет её. 
# Если нет (при локальном запуске), подставит localhost по умолчанию.
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://postgres:postgres@localhost:5432/backend_sandbox"
)

# Создаем асинхронный движок
engine = create_async_engine(DATABASE_URL, echo=True)

# Фабрика сессий (Unit of Work)
async_session_maker = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
  pass


# Dependency для получения сессии в эндпоинтах FastAPI
async def get_db() -> AsyncSession:
  async with async_session_maker() as session:
    yield session