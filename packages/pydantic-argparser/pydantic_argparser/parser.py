import argparse
import logging
from typing import TypeVar, get_origin

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


logger = logging.getLogger(__name__)


def is_list_annotation(dtype: type) -> bool:
    "Check if a type is a list"
    if isinstance(dtype, list):
        return True

    return get_origin(dtype) == list


def add_model(parser: argparse.ArgumentParser, model: type[BaseModel]) -> None:
    "Add Pydantic model to an ArgumentParser"

    for name, field in model.model_fields.items():
        if not field.annotation:
            logger.info(f"Skipping {name} as it has no type annotation")
            continue

        nargs = 1
        if is_list_annotation(field.annotation):
            nargs = "*"

        parser.add_argument(
            f"--{name}",
            dest=name,
            nargs=nargs,
            type=field.annotation,
            default=field.default,
            help=field.description,
        )


def parser_for(model: type[BaseModel]) -> argparse.ArgumentParser:
    "Create an ArgumentParser for a Pydantic model"
    parser = argparse.ArgumentParser()
    add_model(parser, model)
    return parser


def parse_args(model: type[BaseModel]) -> BaseModel:
    "Parse command-line arguments into a Pydantic model"
    parser = parser_for(model)
    return decode_args(parser.parse_args(), model)


def decode_args(parser: argparse.Namespace, model: type[T]) -> T:
    "Decode command-line arguments into a Pydantic model"

    values = dict(vars(parser))
    for name, field in model.model_fields.items():
        if is_list_annotation(field.annotation):
            parser_values = getattr(parser, name)
            value = ["".join(sub_value) for sub_value in parser_values]
            values[name] = value
        elif issubclass(field.annotation, str):
            value = getattr(parser, name)
            if isinstance(value, list):
                value = "".join(value)

            values[name] = value

    return model(**values)
