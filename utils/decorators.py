import logging
import multiprocessing.pool
import time
from functools import wraps
from random import random


logger = logging.getLogger(__name__)


def retry(target_exception=Exception, max_retries=3, max_backoff=5):
    def wrapper_function(function):
        @wraps(function)
        def inner_wrapper(*args, **kwargs):
            retries = max_retries + 1
            while retries:
                retries -= 1
                try:
                    return function(*args, **kwargs)
                except target_exception as exc:
                    logger.warning(
                        f"Error while executing {function}: {exc}. Retrying..."
                    )
                    if retries == 0:
                        raise exc
                    backoff = min((2 ** (retries - 1) + random(), max_backoff))
                    time.sleep(backoff)

        return inner_wrapper

    return wrapper_function


def timeout(timeout_max=30):
    def timeout_decorator(func):
        @wraps(func)
        def timeout_wrapper(*args, **kwargs):
            pool = multiprocessing.pool.ThreadPool(processes=1)
            async_result = pool.apply_async(func, args, kwargs)
            try:
                return async_result.get(timeout_max)
            except multiprocessing.TimeoutError:
                raise TimeoutError(func, timeout_max)

        return timeout_wrapper

    return timeout_decorator
