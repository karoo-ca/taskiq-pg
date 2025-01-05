class BaseTaskiqAsyncpgError(Exception):
    """Base error for all possible exception in the lib."""


class ResultIsMissingError(BaseTaskiqAsyncpgError):
    """Error if cannot retrieve result from PostgreSQL."""
