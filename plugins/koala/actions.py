"""Small collection of importable actions used by ProcessExecutor tests.

Functions are defined here so they are importable by dotted path (e.g. "kola.actions.add").
"""

from __future__ import annotations


def add(a, b):
    return a + b


def mul(x, y):
    return x * y


def slow_const(name: str, delay: float = 0.3):
    import time

    time.sleep(delay)
    return name


def echo(event, state):
    return f"{state}:{event}"
