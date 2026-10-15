from contextlib import asynccontextmanager
import aio_pika
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Глобальная переменная для хранения соединения с брокером
rabbit_connection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
  # --- Startup: выполняется при старте приложения ---
  global rabbit_connection
  print(" Подключение к RabbitMQ...")
  rabbit_connection = await aio_pika.connect("amqp://guest:guest@localhost/")
  print(" Успешно подключено к RabbitMQ!")

  yield

  # --- Shutdown: выполняется при выключении приложения ---
  print(" Закрытие соединения с RabbitMQ...")
  if rabbit_connection:
    await rabbit_connection.close()
  print(" Соединение закрыто.")


app = FastAPI(title="Backend Sandbox - RabbitMQ", lifespan=lifespan)


# Схема данных для входящего POST-запроса
class TaskSchema(BaseModel):
  email: str
  message: str


@app.post("/send-task/")
async def send_task_to_queue(task: TaskSchema):
  if not rabbit_connection:
    raise HTTPException(
        status_code=500, detail="RabbitMQ connection is not established"
    )

  # Открываем канал связи в рамках текущего запроса
  async with rabbit_connection.channel() as channel:
    queue_name = "notification_queue"
    # Объявляем очередь (если она уже есть, просто подключаемся)
    await channel.declare_queue(queue_name, durable=True)

    # Формируем тело сообщения из JSON-запроса
    message_body = f"Email: {task.email}, Text: {task.message}".encode()

    # Публикуем сообщение в очередь
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # Сохраняем при перезапуске брокера
        ),
        routing_key=queue_name,
    )

  return {
      "status": "success",
      "message": f"Задача для {task.email} успешно отправлена в очередь!",
  }