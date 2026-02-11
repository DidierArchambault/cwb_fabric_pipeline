import logging
from typing import Any

from bll.helpers.cleaning_utils.normalize_case import normalize_case
from bll.helpers.cleaning_utils.remove_space import space_remover

logger = logging.getLogger(__name__)


def normalize_naming_conv(value: Any, naming_convention: str = "snake"):
    """
    Normalizes the input string according to the specified naming convention.\n
    Uses space_remover to handle spaces based on the target naming convention,
    and then applies the appropriate case transformation.\n
    Args:
        value: The input value to process.
        naming_convention: The target naming convention. Accepted values are:
            - "snake": snake_case
            - "kebab": kebab-case
            - "pascal": PascalCase
            - "camel": camelCase
    """
    try:
        if isinstance(value, str):
            if naming_convention == "snake":
                value = space_remover(value, "snake").lower()

            if naming_convention == "kebab":
                value = space_remover(value, "kebab").lower()

            elif naming_convention == "pascal":
                parts = value.split(" ")
                value = "".join(p.capitalize() for p in parts if p)

            elif naming_convention == "camel":
                parts = value.split(" ")
                if parts:
                    value = parts[0].lower() + "".join(
                        p.capitalize() for p in parts[1:] if p
                    )
                else:
                    value = value

    except Exception as e:
        logger.error(
            f"Error processing value '{value}': {e}. Returning original value."
        )
    return value
