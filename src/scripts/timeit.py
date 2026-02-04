"""Timing utilities for benchmarking function execution."""

import functools
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")


def timeit(func: Callable[P, R]) -> Callable[P, R]:
    """Decorator that measures and prints function execution time."""

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        t1 = time.perf_counter()
        result = func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f"{func.__name__}() runtime: {(t2 - t1):.4f} seconds")
        return result

    return wrapper
