import logging
from typing import Any

logger = logging.getLogger(__name__)

accepted_naming_conventions = ["snake", "kebab", "pascal", "camel"]


def space_remover(value:Any, replace_with:str = ""):
    """
    Replaces spaces in the input string with the specified replacement.\n
    The fonction is called by normalize_naming_conv but can be used alone.\n
    Args:
        value: The input value to process.
        replace_with: The string to replace spaces with. Accepted values are:
            - "snake": replaces spaces with underscores (_)
            - "kebab": replaces spaces with hyphens (-)
            - "pascal": removes spaces and capitalizes each word
            - "camel": removes spaces, capitalizes each word except the first one
            - "": removes spaces without replacement
    """
    if replace_with not in accepted_naming_conventions and replace_with != "":
        logger.warning(f"Invalid naming convention '{replace_with}' provided. No replacement will be done.")
        replace_with = ""
    try:
        if isinstance(value, str):
            if replace_with is (None or ""):
                return value.replace(" ", "")
            elif replace_with == "kebab":
                return value.replace(" ", "-")
            elif replace_with == "snake":
                return value.replace(" ", "_")
    except Exception as e:
        logger.error(f"Error processing value '{value}': {e}. Returning original value.")
    return value