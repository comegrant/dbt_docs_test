from functools import wraps


def rule_logger():
    def decorator(func):
        @wraps(func)
        def logging_rule_function(possible_dishes, *args, **kwargs):
            return possible_dishes

        return logging_rule_function

    return decorator
