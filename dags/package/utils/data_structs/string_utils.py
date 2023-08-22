def camel_case_to_snake(s: str):
    """
        Replaces camelCase string for snake_case
    """
    return ''.join(['_' + c.lower() if c.isupper() else c for c in s])
