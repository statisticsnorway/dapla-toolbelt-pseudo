"""Common exceptions for the Dapla Pseudo package."""


class NoFileExtensionError(Exception):
    """Exception raised when a file has no file extension."""

    def __init__(self, message: str) -> None:
        """Initialize the NoFileExtensionError."""
        super().__init__(message)


class ExtensionNotValidError(Exception):
    """Exception raised when a file extension is invalid."""

    def __init__(self, message: str) -> None:
        """Initialize the ExtensionNotValidError."""
        super().__init__(message)


class MimetypeNotSupportedError(Exception):
    """Exception raised when a Mimetype is invalid."""

    def __init__(self, message: str) -> None:
        """Initialize the ExtensionNotValidError."""
        super().__init__(message)


class FileInvalidError(Exception):
    """Exception raised when a file is in an invalid state."""

    def __init__(self, message: str) -> None:
        """Initialize the NoFileExtensionError."""
        super().__init__(message)
