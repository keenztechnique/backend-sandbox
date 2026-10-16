from contextlib import asynccontextmanager
import aio_pika
from database import Base, engine, get_db
from fastapi import Depends, FastAPI, HTTPException
from models import TaskModel
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

# Глобальная переменная для соединения с брокером
rabbit_connection = None


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


# Схема данных для входящего POST-запроса
class TaskSchema(BaseModel):
  email: str
  message: str


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
    await db.commit()
    await db.refresh(db_task)

    # 2. Публикуем задачу в RabbitMQ
    async with rabbit_connection.channel() as channel:
      queue_name = "notification_queue"
      await channel.declare_queue(queue_name, durable=True)

      message_body = (
          f"TaskID: {db_task.id}, Email: {task.email}, Text: {task.message}".encode()
      )

      await channel.default_exchange.publish(
          aio_pika.Message(
              body=message_body,
              delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
          ),
          routing_key=queue_name,
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