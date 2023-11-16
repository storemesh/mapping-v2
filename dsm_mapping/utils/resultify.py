class Result:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error

    @classmethod
    def of(cls, func, *args, **kwargs):
        try:
            return cls(value=func(*args, **kwargs))
        except Exception as e:
            return cls(error=str(e))

    @staticmethod
    def success(x):
        return Result(value=x)

    @staticmethod
    def failure(error):
        return Result(error=error)

    def is_error(self):
        return self.error is not None
    
    def bind(self, func):
        if self.is_error():
            return self
        return Result.of(func, self.value)

    def __repr__(self):
        if self.is_error():
            return f"Error({self.error})"
        return f"Success({self.value})"
    
    
def resultify(func):
    def wrapper(*args, **kwargs):
        return Result.of(func, *args, **kwargs)
    return wrapper