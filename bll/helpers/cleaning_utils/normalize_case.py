
import logging
from typing import Any

logger = logging.getLogger(__name__)


def normalize_case(value:Any, case:str = "lower"):
    """
    Normalizes the case of the input string based on the specified case type.
    Args:
        value: The input value to process.
        case: The case type to normalize to. Accepted values are:
            - "lower": converts the string to lowercase
            - "upper": converts the string to uppercase
            - "capitalize": capitalizes the first letter of each word
            - any other value will return the original string without modification
    """
    try:
        if isinstance(value, str):
            if case == "lower":
                return value.lower()
            elif case == "upper":
                return value.upper()
            elif case == "capitalize":
                return value.title()
            else:
                return value
    except Exception as e:
        logger.error(f"Error processing value '{value}': {e}. Returning original value.")
    return value