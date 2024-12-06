import functools
import random

from constants import USER_AGENTS, HEADERS

from logger import get_logger

log = get_logger(__name__)


def retry(max_retries=3, initial_delay=1, max_delay=32):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay

            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        print(f"Max retries reached. Function failed with error: {e}")
                        raise

                    jitter = random.uniform(0, delay)
                    total_delay = min(delay + jitter, max_delay)

                    log.info(
                        f"Retrying in {total_delay:.2f} seconds... (Attempt {retries}/{max_retries}) due to error: {e}"
                    )
                    time.sleep(total_delay)

                    delay = min(delay * 2, max_delay)

        return wrapper

    return decorator_retry


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


def generate_headers() -> dict[str, str]:
    headers = HEADERS
    headers["user-agent"] = get_random_user_agent()
    return headers


def sleep_for_random_n_seconds(min_seconds: int = 15, max_seconds: int = 30) -> None:
    sleep_time = random.uniform(min_seconds, max_seconds)
    log.info(f'Sleeping for {sleep_time=} seconds...')
    time.sleep(sleep_time)


import time


class Timer:
    def __init__(self, message='Elapsed time', unit='seconds'):
        self.unit = unit.lower()
        self.message = message
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        elapsed_time = time.time() - self.start_time

        if self.unit == 'milliseconds':
            elapsed_time *= 1000
            unit_str = "ms"
        else:
            unit_str = "seconds"

        log.info(f"{self.message} time_elapsed_{unit_str}={elapsed_time:.2f}")

