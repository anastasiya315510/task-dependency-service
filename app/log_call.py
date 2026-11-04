import functools
import inspect
import logging
from typing import Any, Callable


def _sanitize(value: Any) -> str:
    """Safe repr — Task objects are reduced to their ID to avoid logging PII."""
    cls_name = type(value).__name__
    if cls_name == "Task":
        return f"Task(id={value.id!r})"
    if isinstance(value, dict) and "id" in value:
        return f"dict(id={value['id']!r}, ...)"
    return repr(value)


def log_call(func: Callable | None = None, *, level: int = logging.DEBUG) -> Callable:
    """Decorator that logs entry, exit, and exceptions for async functions and methods.

    Only supports async functions. Raises TypeError at decoration time if applied
    to a sync function, so the error is caught immediately rather than at call time.

    Usage:
        @log_call
        async def my_func(x): ...

        @log_call(level=logging.INFO)
        async def my_func(x): ...
    """

    def decorator(fn: Callable) -> Callable:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"@log_call only supports async functions; "
                f"{fn.__qualname__!r} is not async"
            )

        _logger = logging.getLogger(fn.__module__)
        _name = fn.__qualname__
        _is_method = "." in _name

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            positional = args[1:] if _is_method else args
            context = f"({_sanitize(positional[0])})" if positional else "()"
            _logger.log(level, "-> %s%s", _name, context)
            try:
                result = await fn(*args, **kwargs)
                _logger.log(level, "<- %s%s", _name, context)
                return result
            except Exception as exc:
                _logger.error("[ERROR] %s%s — %s: %s", _name, context, type(exc).__name__, exc)
                raise

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
