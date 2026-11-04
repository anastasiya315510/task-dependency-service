import logging
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


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
                "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
                datefmt="%H:%M:%S",
            )
        )

    handler.addFilter(_RequestIdFilter())

    root = logging.getLogger()
    root.setLevel(log_level.upper())
    root.handlers = [handler]

    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
