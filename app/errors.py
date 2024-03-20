class ExceptionWithMessage(Exception):
    """A base class for all errors with a message"""

    @property
    def message(self) -> str:
        """
        Returns the message for the exception.

        :return str: The message string.
        """
        return self.args[0] if len(self.args) > 0 else "<no message>"


class RepositoryError(ExceptionWithMessage):
    """Raised when something fails in the database."""

    pass


class ValidationError(ExceptionWithMessage):
    """Raised when validation fails."""

    pass
