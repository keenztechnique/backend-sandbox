# main.py
import datetime
import os
import time
from contextlib import asynccontextmanager
import aio_pika
from database import Base, engine, get_db
from fastapi import Depends, FastAPI, HTTPException, Request
from logger import setup_logging
from models import TaskModel
from pydantic import BaseModel, ConfigDict
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

# Глобальная переменная для соединения с брокером
rabbit_connection = None

setup_logging()
logger = structlog.get_logger()

# Читаем адрес RabbitMQ из переменных окружения (для Docker) или используем localhost по умолчанию
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbit_connection

    print(" Подключение к RabbitMQ...")
    rabbit_connection = await aio_pika.connect(RABBITMQ_URL)
    print(" Успешно подключено к RabbitMQ!")

    yield

    print(" Закрытие соединения с RabbitMQ...")
    if rabbit_connection:
        await rabbit_connection.close()
    print(" Соединение закрыто.")


app = FastAPI(title="Backend Sandbox - DB & RabbitMQ", lifespan=lifespan)


class TaskSchema(BaseModel):
    email: str
    message: str


class TaskResponseSchema(BaseModel):
    id: int
    email: str
    message: str
    status: str
    created_at: datetime.datetime

    model_config = ConfigDict(from_attributes=True)


@app.post("/send-task/")
async def send_task_to_queue(
    task: TaskSchema, db: AsyncSession = Depends(get_db)
):
    if not rabbit_connection:
        raise HTTPException(
            status_code=500, detail="RabbitMQ connection is not established"
        )

    try:
        db_task = TaskModel(
            email=task.email, message=task.message, status="pending"
        )
        db.add(db_task)
        await db.flush()
        await db.refresh(db_task)

        async with rabbit_connection.channel() as channel:
            exchange_name = "notification_exchange"
            routing_key = "notification_routing_key"

            exchange = await channel.declare_exchange(
                exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
            )

            message_body = (
                f"TaskID: {db_task.id}, Email: {task.email}, Text: {task.message}".encode()
            )

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
    result = await db.execute(select(TaskModel).where(TaskModel.id == task_id))
    task = result.scalars().first()

    if not task:
        raise HTTPException(
            status_code=404, detail=f"Задача с ID #{task_id} не найдена"
        )

    return task


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    logger.info("incoming_request", method=request.method, path=request.url.path)

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