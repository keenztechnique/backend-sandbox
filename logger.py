# logger.py
import logging
import sys
import structlog


def setup_logging():
  # Настраиваем стандартный логгер Python
  logging.basicConfig(
      format="%(message)s",
      stream=sys.stdout,
      level=logging.INFO,
  )

  structlog.configure(
      processors=[
          structlog.contextvars.merge_contextvars,
          structlog.processors.add_log_level,
          structlog.processors.StackInfoRenderer(),
          structlog.dev.set_exc_info,
          structlog.processors.TimeStamper(fmt="iso"),
          # Используем ConsoleRenderer для читаемости в консоли во время разработки
          structlog.dev.ConsoleRenderer()
          if True
          else structlog.processors.JSONRenderer(),
      ],
      wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
      context_class=dict,
      logger_factory=structlog.stdlib.LoggerFactory(),
      cache_logger_on_first_use=True,
  )