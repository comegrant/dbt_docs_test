"""A compendium of handy helper functions for case conversions."""


def camel_to_snake(s: str) -> str:
    """
    Convert camelCase string to snake_case

    Args:
        s (str): The camelCase string to convert

    Returns:
        str: The snake_case converted string

    >>> camel_to_snake('camelCase')
    'camel_case'
    >>> camel_to_snake('PascalCase')
    'pascal_case'
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def capital_to_snake(s: str) -> str:
    """
    Convert CAPITALCASE string to snake_case

    Args:
        s (str): The CAPITALCASE string to convert

    Returns:
        str: The snake_case converted string

    >>> capital_to_snake('CAPITAL_CASE')
    'capital_case'
    >>> capital_to_snake('ALL_CAPITAL_CASE')
    'all_capital_case'
    """
    return "_".join(part.lower() for part in s.split("_"))


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
        >>> snake_to_camel('Capital_Case')
        'capitalCase'
        >>> snake_to_camel('Camel_case')
        'camelCase'
        >>> snake_to_camel('ALL_CAPITAL_CASE')
        'allCapitalCase'
        >>> snake_to_camel('CAPS')
        'caps'
    """
    words = s.lower().split("_")
    # Capitalize all words except the first one and if first is capitalised, change it to lower
    return words[0][0].lower() + words[0][1:] + "".join(word.capitalize() for word in words[1:] if word)


def snake_to_pascal(s: str) -> str:
    """
    Convert snake_case string to PascalCase

    Args:
        s (str): The snake_case string to convert

    Returns:
        str: The converted PascalCase string

    >>> snake_to_pascal('pascal_case')
    'PascalCase'
    >>> snake_to_pascal('PASCAL_CAPITAL_CASE')
    'PascalCapitalCase'
    """
    # Capitalize the first letter of all words
    # and lower the rest of the letters
    return "".join([s[0].upper() + s[1:].lower() if s else s for s in s.split("_")])


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=True)
