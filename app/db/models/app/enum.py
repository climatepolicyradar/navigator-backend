import enum


class BaseModelEnum(str, enum.Enum):
    """Family categories as understood in the context of law/policy."""

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and not str.istitle(value):
            return cls(value.title())
        raise ValueError(f"{value} is not a valid {cls.__name__}")

    def __str__(self):
        """Returns tha value."""
        return self._value_
