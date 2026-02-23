from typing import Callable, TypeVar
import logging

logger = logging.getLogger(__name__)


T = TypeVar("T")


def _guard(step: str, fn: Callable[[], T]) -> T:
    """
    Executes a function within a guarded context, logging any exceptions that occur.\n
    :param step: Description of the step being executed.
    :param fn: Function to execute.
    :return: The result of the function if successful.
    """
    try:
        return fn()
    except Exception:
        logger.exception(f" {step}")
        raise
