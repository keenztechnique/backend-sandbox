from contextlib import asynccontextmanager
import aio_pika
from database import Base, engine, get_db
from fastapi import Depends, FastAPI, HTTPException
from models import TaskModel
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession
import datetime
from sqlalchemy import select
import time
from fastapi import FastAPI, Request
import structlog
from logger import setup_logging

# Глобальная переменная для соединения с брокером
rabbit_connection = None

setup_logging()
logger = structlog.get_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
  global rabbit_connection

  # Подключаемся к RabbitMQ (создание таблиц убрано, теперь это делает Alembic)
  print(" Подключение к RabbitMQ...")
  rabbit_connection = await aio_pika.connect("amqp://guest:guest@localhost/")
  print(" Успешно подключено к RabbitMQ!")

  yield

  print(" Закрытие соединения с RabbitMQ...")
  if rabbit_connection:
    await rabbit_connection.close()
  print(" Соединение закрыто.")


app = FastAPI(title="Backend Sandbox - DB & RabbitMQ", lifespan=lifespan)


# Схема для входящего POST-запроса
class TaskSchema(BaseModel):
  email: str
  message: str


class TaskResponseSchema(BaseModel):
  id: int
  email: str
  message: str
  status: str
  created_at: datetime.datetime

  # Современный стандарт Pydantic v2 вместо class Config
  model_config = ConfigDict(from_attributes=True)


@app.post("/send-task/")
async def send_task_to_queue(task: TaskSchema, db: AsyncSession = Depends(get_db)):
  if not rabbit_connection:
    raise HTTPException(
        status_code=500, detail="RabbitMQ connection is not established"
    )

  try:
    # 1. Сохраняем задачу в PostgreSQL через SQLAlchemy сессию
    db_task = TaskModel(email=task.email, message=task.message, status="pending")
    db.add(db_task)
    await db.flush()  # Фиксируем изменения в памяти сессии, получая ID без жесткого commit
    await db.refresh(db_task)

    # 2. Публикуем задачу в RabbitMQ через наш кастомный обменник (Exchange)
    async with rabbit_connection.channel() as channel:
      exchange_name = "notification_exchange"
      routing_key = "notification_routing_key"

      # Объявляем обменник (на всякий случай, если воркер еще не поднялся)
      exchange = await channel.declare_exchange(
          exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
      )

      message_body = (
          f"TaskID: {db_task.id}, Email: {task.email}, Text: {task.message}".encode()
      )

      # Публикуем сообщение в обменник
      await exchange.publish(
          aio_pika.Message(
              body=message_body,
              delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
          ),
          routing_key=routing_key,
      )

    return {
        "status": "success",
        "task_id": db_task.id,
        "message": f"Задача #{db_task.id} сохранена в БД и отправлена в очередь!",
    }

  except Exception as e:
    await db.rollback()
    raise HTTPException(
        status_code=500, detail=f"Ошибка при обработке задачи: {str(e)}"
    )

@app.get("/tasks/{task_id}", response_model=TaskResponseSchema)
async def get_task_status(task_id: int, db: AsyncSession = Depends(get_db)):
  # Выполняем асинхронный запрос к таблице tasks
  result = await db.execute(select(TaskModel).where(TaskModel.id == task_id))
  task = result.scalars().first()

  # Если задача не найдена, возвращаем 404
  if not task:
    raise HTTPException(status_code=404, detail=f"Задача с ID #{task_id} не найдена")

  return task

@app.middleware("http")
async def log_requests(request: Request, call_next):
  start_time = time.time()
  logger.info(
      "incoming_request", method=request.method, path=request.url.path
  )

  response = await call_next(request)

  process_time = (time.time() - start_time) * 1000
  logger.info(
      "request_processed",
      method=request.method,
      path=request.url.path,
      status_code=response.status_code,
      duration_ms=round(process_time, 2),
  )
  return response