class HttpError(Exception):
    def __init__(self, status: int, reason: str, context: str):
        self.status = status
        self.reason = reason
        self.context = context

    def __str__(self):
        return f"Client error '{self.status} {self.reason}'. Context: {self.context}"
