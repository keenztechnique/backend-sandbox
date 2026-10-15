import datetime
from database import Base
from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column


class TaskModel(Base):
  __tablename__ = "tasks"

  id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
  email: Mapped[str] = mapped_column(String(255), nullable=False)
  message: Mapped[str] = mapped_column(Text, nullable=False)
  status: Mapped[str] = mapped_column(String(50), default="pending")
  created_at: Mapped[datetime.datetime] = mapped_column(
      DateTime, default=datetime.datetime.utcnow
  )