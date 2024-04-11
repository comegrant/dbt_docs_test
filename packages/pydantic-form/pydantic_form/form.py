from collections.abc import Callable
from datetime import timedelta
from types import UnionType
from typing import Any, TypeVar, get_args

import streamlit as st
from pydantic import BaseModel
from pydantic.fields import FieldInfo

T = TypeVar("T", bound=BaseModel)


def list_input(name: str, info: FieldInfo) -> list[str] | None:
    value = ",".join(info.default) if isinstance(info.default, list) else info.default

    values = st.text_input(name, value=value)
    st.caption(f"Comma separated list of {name}")
    if not values:
        return None
    return str(values).split(",")


def default_value(info: FieldInfo) -> Any:  # noqa: ANN401
    if info.default_factory:
        return info.default_factory()

    value = info.default
    if str(value) == "PydanticUndefined":
        return None

    if isinstance(value, timedelta):
        return value.total_seconds()
    return value


streamlit_components: dict[str, Callable[[str, FieldInfo], Any]] = {
    "str": lambda name, info: st.text_input(name, value=default_value(info)),
    "list": lambda name, info: list_input(name, info),
    "int": lambda name, info: st.number_input(name, value=default_value(info), step=1),
    "bool": lambda name, info: st.checkbox(name, value=default_value(info)),
    "timedelta": lambda name, info: st.number_input(name, value=default_value(info)),
    "Optional": lambda name, info: st.text_input(name),
}


def pydantic_form(key: str, model: type[T]) -> T | None:
    values = {}
    required_fields = set()
    optional_union_type_length = 2

    with st.form(key=key):
        for name, field in model.model_fields.items():
            annotation = field.annotation
            is_optional = False

            if isinstance(annotation, UnionType):
                sub_types = list(get_args(annotation))

                if len(sub_types) == optional_union_type_length and type(None) in sub_types:
                    is_optional = True
                    annotation = sub_types[0] if sub_types[0] != type(None) else sub_types[1]

            type_name = annotation.__name__
            component = streamlit_components[type_name]

            if not is_optional:
                required_fields.add(name)
                values[name] = component(f"{name} - Required", field)
            else:
                values[name] = component(name, field)

        st.form_submit_button()

    for field in required_fields:
        if values[field] is None or values[field] == "" or values[field] == "\n":
            return None

    return model(**values)
