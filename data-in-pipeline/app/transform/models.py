class CouldNotTransform(Exception):
    """Raised when a transformer function cannot be found for the given input."""

    pass


class NoMatchingTransformations(Exception):
    """Raised when the transformer finds no matching transformations for the given input."""

    pass
