# worker.py
import asyncio
import json
import structlog
from aio_pika import ExchangeType, IncomingMessage, connect
from logger import setup_logging

# Инициализируем структурированное логирование
setup_logging()
logger = structlog.get_logger()


async def process_task(message: IncomingMessage):
  # Получаем информацию о количестве неудачных попыток из заголовков x-death
  headers = message.headers or {}
  x_death = headers.get("x-death")

  retry_count = 0
  if x_death:
    retry_count = x_death[0].get("count", 0)

  max_retries = 3  # максимальное число попыток
  attempt_num = retry_count + 1

  # Декодируем тело сообщения заранее, чтобы попытаться достать task_id/email для логов
  try:
    body_text = message.body.decode()
    body_data = json.loads(body_text)
    task_id = body_data.get("id") or body_data.get("task_id")
    email = body_data.get("email")
  except Exception:
    body_text = message.body.decode()
    task_id = None
    email = None

  logger.info(
      "task_received_in_worker",
      attempt=attempt_num,
      max_retries=max_retries,
      task_id=task_id,
      email=email,
      body=body_text,
  )

  try:
    # Имитируем работу (и иногда намеренно вызываем сбой для теста)
    await asyncio.sleep(2)

    # Демонстрация ошибки: если текст содержит "error", падаем
    if "error" in body_text.lower():
      raise ValueError("Искусственная ошибка обработки задачи!")

    # Если всё успешно, подтверждаем сообщение (ACK)
    await message.ack()
    logger.info("task_completed_successfully", task_id=task_id)

  except Exception as e:
    logger.error(
        "task_processing_failed",
        task_id=task_id,
        attempt=attempt_num,
        error=str(e),
        exc_info=True,
    )

    if retry_count < max_retries:
      # Отклоняем сообщение без возврата в текущую очередь (requeue=False).
      # Оно автоматически полетит в DLX (очередь повтора с задержкой).
      await message.nack(requeue=False)
      logger.warning(
          "task_scheduled_for_retry",
          task_id=task_id,
          next_attempt=attempt_num + 1,
          max_retries=max_retries,
      )
    else:
      # Превышен лимит попыток — отправляем в Dead Letter Queue навсегда
      await message.reject(requeue=False)
      logger.error(
          "task_retries_exhausted_sent_to_dlq",
          task_id=task_id,
          max_retries=max_retries,
      )


async def main():
  connection = await connect("amqp://guest:guest@localhost/")
  channel = await connection.channel()

  await channel.set_qos(prefetch_count=1)

  # 1. Объявляем обменник для повторных попыток (Retry Exchange)
  retry_exchange = await channel.declare_exchange(
      "notification_retry_exchange", ExchangeType.DIRECT, durable=True
  )

  # 2. Объявляем основную точку обмена
  main_exchange = await channel.declare_exchange(
      "notification_exchange", ExchangeType.DIRECT, durable=True
  )

  # 3. Объявляем очередь повтора (Retry Queue) с задержкой (TTL = 5000 мс / 5 секунд)
  retry_queue = await channel.declare_queue(
      "notification_retry_queue",
      durable=True,
      arguments={
          "x-message-ttl": 5000,
          "x-dead-letter-exchange": "notification_exchange",
          "x-dead-letter-routing-key": "notification_routing_key",
      },
  )
  await retry_queue.bind(retry_exchange, routing_key="retry_routing_key")

  # 4. Объявляем основную очередь задач (Notification Queue) с привязкой DLX
  queue = await channel.declare_queue(
      "notification_queue",
      durable=True,
      arguments={
          "x-dead-letter-exchange": "notification_retry_exchange",
          "x-dead-letter-routing-key": "retry_routing_key",
      },
  )
  await queue.bind(main_exchange, routing_key="notification_routing_key")

  logger.info("worker_started_waiting_for_messages")

  await queue.consume(process_task)

  try:
    await asyncio.Future()
  finally:
    await connection.close()


if __name__ == "__main__":
  asyncio.run(main())