import logging
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")

# Tracks the handler we installed so subsequent configure_logging() calls
# replace only our own handler, leaving third-party handlers (Sentry, etc.) intact.
# Using a dict avoids module-level reassignment (global statement).
_state: dict = {"handler": None}


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
                "%(asctime)s %(levelname)-8s [%(request_id)s] %(name)s | %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    handler.addFilter(_RequestIdFilter())

    root = logging.getLogger()
    root.setLevel(log_level.upper())
    # Always remove our previously-installed handler before adding the new one.
    # This covers both the normal re-call path (format/level change) and the
    # edge case where a third-party library removed it — in that case the remove
    # is a silent no-op and we still add exactly one new handler, avoiding
    # duplicates even if configure_logging() is called multiple times.
    if _state["handler"] is not None:
        root.removeHandler(_state["handler"])
    root.addHandler(handler)
    _state["handler"] = handler

    # Set third-party logger levels on every call so re-calls with a different
    # log_level are idempotent (NOTSET = inherit from root = no suppression).
    third_party_level = logging.NOTSET if log_level.upper() == "DEBUG" else logging.WARNING
    for _name in ("aiokafka", "motor", "pymongo"):
        logging.getLogger(_name).setLevel(third_party_level)
