# tests/test_tasks.py
import pytest


@pytest.mark.asyncio
async def test_create_and_get_task(async_client):
  # 1. Отправляем POST-запрос на создание задачи
  response = await async_client.post(
      "/send-task/",
      json={"email": "pytest_user@example.com", "message": "Тестовое сообщение"},
  )

  print("\nОТВЕТ POST:", response.status_code, response.json())

  assert response.status_code in [200, 201]
  data = response.json()
  assert data["status"] == "success"
  assert "task_id" in data

  task_id = data["task_id"]
  print("ПОЛУЧЕННЫЙ TASK_ID:", task_id)

  # 2. Делаем GET-запрос для проверки статуса созданной задачи
  get_response = await async_client.get(f"/tasks/{task_id}")
  print("ОТВЕТ GET:", get_response.status_code, get_response.text)

  assert get_response.status_code == 200
  task_data = get_response.json()
  assert task_data["id"] == task_id
  assert task_data["email"] == "pytest_user@example.com"
  assert task_data["message"] == "Тестовое сообщение"
  assert task_data["status"] == "pending"