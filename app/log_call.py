import functools
import inspect
import logging
from typing import Any, Callable


def sanitize(value: Any) -> str:
    """Safe repr — objects with an 'id' field are reduced to avoid logging PII.

    Assumption: any future domain object that carries PII must have both 'id'
    and 'title' attributes so the duck-type guard above catches it. Objects that
    don't match any guard fall through to repr() — add a guard before adding
    new domain types that contain user-facing strings.
    """
    # Duck-typed check: match objects that look like Task (id + title + parent_task_id).
    # The three-attribute guard is specific enough to avoid false positives on
    # framework objects (e.g. Request) that also carry 'id' or 'title'.
    # No import of app.models keeps this module free of domain dependencies.
    if isinstance(value, bytes):
        return f"<bytes len={len(value)}>"
    if hasattr(value, "id") and hasattr(value, "title") and hasattr(value, "parent_task_id"):
        return f"{type(value).__name__}(id={value.id!r})"
    if isinstance(value, dict) and "id" in value:
        return f"dict(id={value['id']!r}, ...)"
    if isinstance(value, list):
        return f"[{len(value)} item(s)]"
    if isinstance(value, tuple):
        return f"({', '.join(sanitize(v) for v in value)})"
    return repr(value)


def log_call(func: Callable | None = None, *, level: int = logging.DEBUG) -> Callable:
    """Decorator that logs entry and exit for async functions and methods.

    Exceptions are intentionally not logged here — the caller or a top-level
    exception handler is responsible for logging exceptions to avoid duplicate
    log entries when multiple decorated functions are on the same call stack.

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
        # Detect methods by inspecting the first parameter name rather than
        # checking __qualname__ for a ".", which is a false positive for
        # closures/nested functions (e.g. outer.<locals>.inner).
        _params = list(inspect.signature(fn).parameters.values())
        _is_method = bool(_params) and _params[0].name in ("self", "cls")

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            positional = args[1:] if _is_method else args
            parts = [sanitize(a) for a in positional] + [
                f"{k}={sanitize(v)}" for k, v in kwargs.items()
            ]
            context = f"({', '.join(parts)})" if parts else "()"
            _logger.log(level, "-> %s%s", _name, context)
            result = await fn(*args, **kwargs)
            _logger.log(level, "<- %s%s = %s", _name, context, sanitize(result))
            return result

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
