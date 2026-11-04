import logging
from contextvars import ContextVar
from typing import Optional

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

# Tracks the handler we installed so subsequent configure_logging() calls
# replace only our own handler, leaving third-party handlers (Sentry, etc.) intact.
_installed_handler: Optional[logging.Handler] = None


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()  # type: ignore[attr-defined]
        return True


def configure_logging(log_level: str = "DEBUG", log_format: str = "text") -> None:
    handler = logging.StreamHandler()

    if log_format.lower() == "json":
        from pythonjsonlogger.jsonlogger import JsonFormatter  # pylint: disable=import-outside-toplevel

        handler.setFormatter(
            JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s"
            )
        )
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(name)s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    handler.addFilter(_RequestIdFilter())

    global _installed_handler
    root = logging.getLogger()
    root.setLevel(log_level.upper())
    # Replace our own handler on re-calls (e.g. format change) but preserve
    # any third-party handlers (Sentry, etc.) added before configure_logging().
    if _installed_handler is not None and _installed_handler in root.handlers:
        root.handlers[root.handlers.index(_installed_handler)] = handler
    else:
        root.addHandler(handler)
    _installed_handler = handler

    # Set third-party logger levels on every call so re-calls with a different
    # log_level are idempotent (NOTSET = inherit from root = no suppression).
    third_party_level = logging.NOTSET if log_level.upper() == "DEBUG" else logging.WARNING
    for _name in ("aiokafka", "motor", "pymongo"):
        logging.getLogger(_name).setLevel(third_party_level)
