import argparse
import logging
from datetime import date, datetime
from types import UnionType
from typing import Literal, TypeVar, get_args, get_origin

from pydantic import BaseModel
from pydantic_core import PydanticUndefined

T = TypeVar("T", bound=BaseModel)


logger = logging.getLogger(__name__)


def is_list_annotation(dtype: type) -> bool:
    "Check if a type is a list"
    if isinstance(dtype, list):
        return True

    return get_origin(dtype) is list


optional_union_type_langth = 2


def add_model(parser: argparse.ArgumentParser, model: type[BaseModel]) -> None:
    "Add Pydantic model to an ArgumentParser"

    for name, field in model.model_fields.items():
        if not field.annotation:
            logger.info(f"Skipping {name} as it has no type annotation")
            continue

        if field.annotation is datetime:
            field.annotation = str

        if field.annotation is date:
            field.annotation = str

        annotation = field.annotation

        if isinstance(field.annotation, UnionType):
            sub_types = list(get_args(field.annotation))

            if len(sub_types) == optional_union_type_langth and type(None) in sub_types:
                annotation = sub_types[0] if sub_types[0] is not type(None) else sub_types[1]
        elif get_origin(annotation) == Literal:
            annotation = type(get_args(annotation)[0])

        nargs = 1
        if is_list_annotation(annotation):
            nargs = "*"

        default_value = None
        if field.default_factory:
            default_value = field.default_factory()  # type: ignore
        elif field.default != PydanticUndefined:
            default_value = field.default

        assert annotation is not None, f"Found no annotation for {field}"

        parser.add_argument(
            f"--{name.replace('_', '-')}",
            dest=name,
            nargs=nargs,
            type=annotation,
            default=default_value,
            help=field.description,
        )


def parser_for(model: type[BaseModel]) -> argparse.ArgumentParser:
    "Create an ArgumentParser for a Pydantic model"
    parser = argparse.ArgumentParser()
    add_model(parser, model)
    return parser


def parse_args(model: type[T]) -> T:
    "Parse command-line arguments into a Pydantic model"
    parser = parser_for(model)
    return decode_args(parser.parse_args(), model)


def decode_args(parser: argparse.Namespace, model: type[T]) -> T:
    "Decode command-line arguments into a Pydantic model"

    values = dict(vars(parser))
    for name, field in model.model_fields.items():
        value = getattr(parser, name)

        if value == PydanticUndefined:
            raise ValueError(f"Got an undefined value for '{name}'")

        if value is None:
            continue

        if field.annotation is None:
            continue

        if isinstance(field.annotation, UnionType):
            sub_types = list(get_args(field.annotation))

            if len(sub_types) == optional_union_type_langth and type(None) in sub_types:
                annotation = next(sub_type for sub_type in sub_types if sub_type is not None)
            else:
                annotation = field.annotation
        else:
            annotation = field.annotation

        if is_list_annotation(annotation) and value:
            value = ["".join(sub_value) for sub_value in value]
            values[name] = value
        elif annotation is str or get_origin(annotation) == Literal:
            values[name] = "".join(value)
        elif isinstance(value, list):
            values[name] = value[0]
        else:
            values[name] = value

    return model(**values)
