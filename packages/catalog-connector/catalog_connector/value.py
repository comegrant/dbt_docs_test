from __future__ import annotations

from dataclasses import dataclass, field


class ValueRepresentable:
    def read(self) -> str:
        raise NotImplementedError(type(self))

    @staticmethod
    def from_value(value: str | ValueRepresentable) -> ValueRepresentable:
        if isinstance(value, ValueRepresentable):
            return value
        else:
            return LiteralValue(value)


@dataclass
class EnvironmentValue(ValueRepresentable):
    env: str
    default_value: str | None = field(default=None)

    def read(self) -> str:
        import os

        if self.default_value and self.env not in os.environ:
            return self.default_value

        try:
            return os.environ[self.env]
        except KeyError as e:
            raise ValueError(f"Environment variable '{self.env}' is not set and no default value was provided") from e


@dataclass
class LiteralValue(ValueRepresentable):
    value: str

    def read(self) -> str:
        return self.value
