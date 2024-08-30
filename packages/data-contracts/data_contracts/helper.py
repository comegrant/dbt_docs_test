""" A compendium of handy helper functions """


def camel_to_snake(s: str) -> str:
    """
    Convert camelCase string to snake_case

    Args:
        s (str): The camelCase string to convert

    Returns:
        str: The snake_case converted string

    >>> camel_to_snake('camelCase')
    'camel_case'
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def snake_to_camel(s: str) -> str:
    """
    Convert snake_case string to camelCase.

    Args:
        s (str): The snake_case string to convert.

    Returns:
        str: The converted camelCase string.

    Example:
        >>> snake_to_camel('snake_case_string')
        'snakeCaseString'
    """
    words = s.split("_")
    # Capitalize all words except the first one
    return words[0] + "".join(word.capitalize() for word in words[1:] if word)


def snake_to_pascal(s: str) -> str:
    """
    Convert snake_case string to PascalCase

    Args:
        s (str): The snake_case string to convert

    Returns:
        str: The converted PascalCase string

    >>> snake_to_pascal('camel_case')
    'CamelCase'
    """
    # Capitalize the first letter of all words
    return "".join([s[0].upper() + s[1:] if s else s for s in s.split("_")])
