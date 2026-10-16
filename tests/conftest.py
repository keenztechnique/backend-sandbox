# tests/conftest.py
from unittest.mock import AsyncMock, MagicMock
from database import Base, engine, get_db
from httpx import ASGITransport, AsyncClient
import main
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_db_and_transaction():
  """Создает соединения, открывает транзакцию на время теста и откатывает её"""
  async with engine.connect() as connection:
    # Начинаем транзакцию для теста
    transaction = await connection.begin()

    # Создаем фабрику сессий, привязанную к этому же коннекту
    async_session_test = async_sessionmaker(
        bind=connection, class_=AsyncSession, expire_on_commit=False
    )

    # Переопределяем get_db, чтобы приложение использовало нашу тестовую сессию
    async def override_get_db() -> AsyncSession:
      async with async_session_test() as session:
        yield session

    main.app.dependency_overrides[get_db] = override_get_db

    yield  # Здесь выполняется сам тест

    # Откатываем транзакцию после теста, чтобы база осталась чистой
    await transaction.rollback()


@pytest_asyncio.fixture(autouse=True)
def mock_rabbitmq(monkeypatch):
  """Мок для RabbitMQ"""
  mock_channel = AsyncMock()
  mock_exchange = AsyncMock()
  mock_channel.default_exchange = mock_exchange

  mock_context_manager = AsyncMock()
  mock_context_manager.__aenter__.return_value = mock_channel

  mock_connection = AsyncMock()
  mock_connection.channel = MagicMock(return_value=mock_context_manager)

  monkeypatch.setattr(main, "rabbit_connection", mock_connection)


@pytest_asyncio.fixture(scope="function")
async def async_client():
  """Клиент для асинхронных HTTP-запросов"""
  async with AsyncClient(
      transport=ASGITransport(app=main.app), base_url="http://test"
  ) as ac:
    yield ac