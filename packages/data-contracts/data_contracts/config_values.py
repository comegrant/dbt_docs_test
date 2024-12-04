from __future__ import annotations

from dataclasses import dataclass, field

from aligned.schemas.codable import Codable
from mashumaro.types import SerializableType


class ValueRepresentable(Codable, SerializableType):

    type_name: str

    def read(self) -> str:
        ...

    def _serialize(self) -> dict:
        assert (
            self.type_name in SupportedValueFactory.shared().supported_values
        ), f'Unknown type_name: {self.type_name}'
        return self.to_dict()

    @classmethod
    def _deserialize(cls, value: dict) -> ValueRepresentable:
        name_type = value['type_name']
        if name_type not in SupportedValueFactory.shared().supported_values:
            raise ValueError(
                f"Unknown batch data source id: '{name_type}'.\nRemember to add the"
                ' data source to the SupportedValueFactory.supported_values if'
                ' it is a custom type.'
            )
        del value['type_name']
        data_class = SupportedValueFactory.shared().supported_values[name_type]
        return data_class.from_dict(value)


class SupportedValueFactory:

    supported_values: dict[str, type[ValueRepresentable]]

    _shared: SupportedValueFactory | None = None

    def __init__(self) -> None:
        types = [EnvironmentValue, LiteralValue]

        self.supported_values = {
            val_type.type_name: val_type
            for val_type in types
        }

    @classmethod
    def shared(cls) -> SupportedValueFactory:
        if cls._shared:
            return cls._shared
        cls._shared = SupportedValueFactory()
        return cls._shared


@dataclass
class EnvironmentValue(ValueRepresentable, Codable):

    env: str
    default_value: str | None = field(default=None)
    type_name: str = "env"

    def read(self) -> str:
        import os
        if self.default_value and self.env not in os.environ:
            return self.default_value

        return os.environ[self.env]


@dataclass
class LiteralValue(ValueRepresentable, Codable):

    value: str
    type_name = "literal"

    def read(self) -> str:
        return self.value

    @staticmethod
    def from_value(value: str | ValueRepresentable) -> ValueRepresentable:
        if isinstance(value, ValueRepresentable):
            return value
        else:
            return LiteralValue(value)
