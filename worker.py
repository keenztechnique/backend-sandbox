import asyncio
from aio_pika import IncomingMessage, connect


async def main():
  # Подключаемся к RabbitMQ
  connection = await connect("amqp://guest:guest@localhost/")
  channel = await connection.channel()

  # Ограничиваем QoS: один воркер берет по 1 сообщению за раз
  await channel.set_qos(prefetch_count=1)

  queue = await channel.declare_queue("notification_queue", durable=True)
  print(
      " [*] Воркер запущен и ожидает новые задачи. Для выхода нажмите CTRL+C"
  )

  async def process_message(message: IncomingMessage):
    async with message.process():
      print(f" [x] Получена задача: {message.body.decode()}")
      # Имитируем долгую фоновую работу (например, отправку письма через SMTP)
      await asyncio.sleep(4)
      print(" [x] Письмо успешно отправлено (задача выполнена)!")

  await queue.consume(process_message)

  try:
    await asyncio.Future()
  finally:
    await connection.close()


if __name__ == "__main__":
  asyncio.run(main())