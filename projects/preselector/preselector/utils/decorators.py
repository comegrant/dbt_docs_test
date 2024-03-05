from functools import wraps


def rule_logger():
    def decorator(func):
        @wraps(func)
        def logging_rule_function(possible_dishes, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ARG001
            return possible_dishes

        return logging_rule_function

    return decorator
