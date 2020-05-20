import functools

def retry_on_exception(max_retries=5):
    """
        Call function up to max_retries if raise exception
        Use it as decorator
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            last_except = None
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  #TODO: Use a more specific exception??
                    retries += 1
                    last_except = e

            if last_except is not None:
                raise last_except

        return wrapper
    return decorator