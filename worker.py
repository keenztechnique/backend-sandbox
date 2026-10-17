# worker.py
import asyncio
from aio_pika import ExchangeType, IncomingMessage, Message, connect


async def process_task(message: IncomingMessage):
  # Получаем информацию о количестве неудачных попыток из заголовков x-death
  headers = message.headers or {}
  x_death = headers.get("x-death")

  retry_count = 0
  if x_death:
    # x-death хранит историю пересылок сообщения между очередями
    retry_count = x_death[0].get("count", 0)

  max_retries = 3  # максимальное число попыток

  print(
      f" [x] Получено сообщение. Попытка №{retry_count + 1} (из {max_retries})"
  )

  try:
    body = message.body.decode()
    print(f" [x] Текст задачи: {body}")

    # Имитируем работу (и иногда намеренно вызываем сбой для теста)
    await asyncio.sleep(2)

    # Демонстрация ошибки: если текст содержит "error", падаем
    if "error" in body.lower():
      raise ValueError("Искусственная ошибка обработки задачи!")

    # Если всё успешно, подтверждаем сообщение (ACK)
    await message.ack()
    print(" [✔] Задача успешно выполнена!")

  except Exception as e:
    print(f" [!] Ошибка при обработке: {e}")

    if retry_count < max_retries:
      # Отклоняем сообщение без возврата в текущую очередь (requeue=False).
      # Оно автоматически полетит в DLX (очередь повтора с задержкой).
      await message.nack(requeue=False)
      print(
          f" [🔄] Ошибка временная. Сообщение отправлено на повтор (попытка"
          f" {retry_count + 1}/{max_retries})"
      )
    else:
      # Превышен лимит попыток — отправляем в Dead Letter Queue навсегда (или сбрасываем)
      # Здесь можно отправить в отдельную очередь финальных ошибок
      await message.reject(requeue=False)
      print(
          " [❌] Превышен лимит попыток! Сообщение отправлено в Dead Letter"
          " Queue."
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
  # Когда время истекает, сообщение возвращается обратно в main_exchange
  retry_queue = await channel.declare_queue(
      "notification_retry_queue",
      durable=True,
      arguments={
          "x-message-ttl": 5000,  # Задержка перед повтором в миллисекундах (5 сек)
          "x-dead-letter-exchange": "notification_exchange",  #Куда отправить после TTL
          "x-dead-letter-routing-key": "notification_routing_key",
      },
  )
  await retry_queue.bind(retry_exchange, routing_key="retry_routing_key")

  # 4. Объявляем основную очередь задач (Notification Queue) с привязкой DLX
  # Если сообщение отклонено (nack с requeue=False), оно улетает в notification_retry_exchange
  queue = await channel.declare_queue(
      "notification_queue",
      durable=True,
      arguments={
          "x-dead-letter-exchange": "notification_retry_exchange",
          "x-dead-letter-routing-key": "retry_routing_key",
      },
  )
  await queue.bind(main_exchange, routing_key="notification_routing_key")

  print(" [*] Воркер с поддержкой Retry/DLX запущен и ожидает задачи...")

  await queue.consume(process_task)

  try:
    await asyncio.Future()
  finally:
    await connection.close()


if __name__ == "__main__":
  asyncio.run(main())