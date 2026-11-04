"""Unit tests for the log_call decorator and _sanitize helper."""
import logging

import pytest

from app.log_call import _sanitize, log_call
from app.models import Task

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# _sanitize
# ---------------------------------------------------------------------------

def test_sanitize_task_returns_id_only():
    t = Task(id="t1", title="Sensitive Title", parent_task_id=None)
    result = _sanitize(t)
    assert result == "Task(id='t1')"
    assert "Sensitive Title" not in result


def test_sanitize_dict_with_id_key_shows_id_only():
    result = _sanitize({"id": "abc", "title": "Sensitive"})
    assert "abc" in result
    assert "Sensitive" not in result


def test_sanitize_dict_without_id_key_falls_through_to_repr():
    result = _sanitize({"foo": "bar"})
    assert "foo" in result


def test_sanitize_list_shows_length_not_contents():
    t = Task(id="t1", title="Sensitive", parent_task_id=None)
    assert _sanitize([t, t, t]) == "[3 item(s)]"
    assert "Sensitive" not in _sanitize([t])


def test_sanitize_empty_list():
    assert _sanitize([]) == "[0 item(s)]"


def test_sanitize_tuple_recurses_per_element():
    t = Task(id="t1", title="Sensitive", parent_task_id=None)
    result = _sanitize(([t, t], True, False))
    assert "Sensitive" not in result
    assert "[2 item(s)]" in result
    assert "True" in result
    assert "False" in result


def test_sanitize_none():
    assert _sanitize(None) == "None"


def test_sanitize_int():
    assert _sanitize(42) == "42"


def test_sanitize_string():
    assert _sanitize("hello") == "'hello'"


# ---------------------------------------------------------------------------
# log_call — decoration-time checks
# ---------------------------------------------------------------------------

def test_log_call_raises_for_sync_function():
    with pytest.raises(TypeError, match="not async"):
        @log_call
        def sync_func():
            pass


def test_log_call_raises_for_sync_method():
    with pytest.raises(TypeError, match="not async"):
        class MyClass:
            @log_call
            def sync_method(self):
                pass


# ---------------------------------------------------------------------------
# log_call — runtime logging behaviour
# ---------------------------------------------------------------------------

async def test_log_call_logs_entry_with_args(caplog):
    @log_call
    async def greet(name):
        return f"hello {name}"

    with caplog.at_level(logging.DEBUG):
        await greet("world")

    assert "greet" in caplog.text
    assert "'world'" in caplog.text


async def test_log_call_logs_exit_with_return_value(caplog):
    @log_call
    async def compute():
        return 99

    with caplog.at_level(logging.DEBUG):
        await compute()

    assert "compute" in caplog.text
    assert "= 99" in caplog.text


async def test_log_call_return_value_is_unchanged():
    expected = object()

    @log_call
    async def identity():
        return expected

    assert await identity() is expected


async def test_log_call_omits_self_from_entry_log(caplog):
    class MyService:
        @log_call
        async def run(self, task_id: str):
            return None

    with caplog.at_level(logging.DEBUG):
        await MyService().run("t1")

    # 't1' appears; the repr of self (contains 'object at 0x') does not
    assert "'t1'" in caplog.text
    assert "object at 0x" not in caplog.text


async def test_log_call_sanitizes_task_argument(caplog):
    t = Task(id="t99", title="Super Secret", parent_task_id=None)

    @log_call
    async def handle(task):
        return None

    with caplog.at_level(logging.DEBUG):
        await handle(t)

    assert "Super Secret" not in caplog.text
    assert "t99" in caplog.text


async def test_log_call_sanitizes_list_return_value(caplog):
    t = Task(id="t1", title="Sensitive", parent_task_id=None)

    @log_call
    async def fetch_all():
        return [t, t]

    with caplog.at_level(logging.DEBUG):
        await fetch_all()

    assert "Sensitive" not in caplog.text
    assert "2 item(s)" in caplog.text


async def test_log_call_sanitizes_tuple_return_value(caplog):
    t = Task(id="t1", title="Sensitive", parent_task_id=None)

    @log_call
    async def fetch_chain():
        return ([t], True, False)

    with caplog.at_level(logging.DEBUG):
        await fetch_chain()

    assert "Sensitive" not in caplog.text
    assert "1 item(s)" in caplog.text
    assert "True" in caplog.text


async def test_log_call_reraises_exception():
    @log_call
    async def broken():
        raise RuntimeError("kaboom")

    with pytest.raises(RuntimeError, match="kaboom"):
        await broken()


async def test_log_call_accepts_custom_log_level(caplog):
    @log_call(level=logging.INFO)
    async def info_func():
        return None

    with caplog.at_level(logging.INFO):
        await info_func()

    entry_records = [
        r for r in caplog.records
        if r.levelno == logging.INFO and "->" in r.getMessage()
    ]
    assert entry_records