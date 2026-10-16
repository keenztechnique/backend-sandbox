# worker.py
import asyncio
from aio_pika import IncomingMessage, connect
from database import async_session_maker
from models import TaskModel
from sqlalchemy import select


async def process_task(message: IncomingMessage):
  async with message.process():
    body = message.body.decode()
    print(f" [x] Получено сырое сообщение: {body}")

    # Ожидаемый формат сообщения от FastAPI: "TaskID: 1, Email: ..., Text: ..."
    # Вытащим оттуда Task ID для работы с базой данных
    try:
      task_id_str = body.split(",")[0].replace("TaskID:", "").strip()
      task_id = int(task_id_str)
    except Exception as e:
      print(f" [!] Ошибка парсинга ID задачи: {e}")
      return

    # Открываем сессию базы данных внутри воркера (паттерн Unit of Work)
    async with async_session_maker() as session:
      async with session.begin():
        # 1. Находим задачу в БД по ID
        result = await session.execute(
            select(TaskModel).where(TaskModel.id == task_id)
        )
        task = result.scalars().first()

        if not task:
          print(f" [!] Задача с ID {task_id} не найдена в базе данных!")
          return

        # 2. Меняем статус на 'processing' (в работе)
        task.status = "processing"
        await session.commit()
        print(f" [⚙️] Задача #{task_id} взята в работу (статус: processing)")

      # Имитируем долгую фоновую работу (например, отправку письма или тяжелые вычисления)
      await asyncio.sleep(4)

      async with session.begin():
        # Повторно затягиваем объект в новую транзакцию для обновления статуса
        result = await session.execute(
            select(TaskModel).where(TaskModel.id == task_id)
        )
        task = result.scalars().first()

        if task:
          # 3. Меняем статус на 'completed' (выполнено)
          task.status = "completed"
          await session.commit()
          print(
              f" [✔] Задача #{task_id} успешно выполнена и обновлена в БД!"
          )


async def main():
  # Подключаемся к RabbitMQ
  connection = await connect("amqp://guest:guest@localhost/")
  channel = await connection.channel()

  # Ограничиваем QoS: один воркер берет строго по 1 сообщению за раз
  await channel.set_qos(prefetch_count=1)

  # Подписываемся на очередь
  queue = await channel.declare_queue("notification_queue", durable=True)
  print(
      " [*] Воркер с поддержкой БД запущен и ожидает задачи. Для выхода нажмите"
      " CTRL+C"
  )

  # Запускаем прослушивание очереди
  await queue.consume(process_task)

  # Держим процесс запущенным
  try:
    await asyncio.Future()
  finally:
    await connection.close()


if __name__ == "__main__":
  asyncio.run(main())