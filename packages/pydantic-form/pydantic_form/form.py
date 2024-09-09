import json
import logging
from collections.abc import Callable
from datetime import timedelta
from types import UnionType
from typing import Any, TypeVar, get_args

import streamlit as st
from pydantic import BaseModel
from pydantic.fields import FieldInfo

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)


def list_input(name: str, info: FieldInfo, is_required: bool) -> list[str] | None:
    value = ",".join([str(val) for val in info.default]) if isinstance(info.default, list) else info.default

    value = info.default
    if str(value) == "PydanticUndefined":
        value = None

    values = st.text_input(name, value=value)
    st.caption(f"Comma separated list of {name}")

    if not values and is_required:
        return []
    if not values:
        return None

    if not values.startswith("["):
        values = f"[{values}]"

    try:
        return json.loads(values)
    except json.JSONDecodeError:
        logging.info(f"Could not parse {name} as JSON, returning as string")
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


streamlit_components: dict[str, Callable[[str, FieldInfo, bool], Any]] = {
    "str": lambda name, info, is_required: st.text_input(name, value=default_value(info)),
    "list": lambda name, info, is_required: list_input(name, info, is_required),
    "int": lambda name, info, is_required: st.number_input(name, value=default_value(info), step=1),
    "bool": lambda name, info, is_required: st.checkbox(name, value=default_value(info)),
    "timedelta": lambda name, info, is_required: st.number_input(name, value=default_value(info)),
    "Optional": lambda name, info, is_required: st.text_input(name),
}


def pydantic_form(key: str, model: type[T], wait_for_submit: bool = False) -> T | None:
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
                values[name] = component(f"{name} - Required", field, not is_optional)
            else:
                values[name] = component(name, field, not is_optional)

        is_submitted = st.form_submit_button()

    has_run_key = f"{key}_has_run"
    if not st.session_state.get(has_run_key, False) and (wait_for_submit and not is_submitted):
        st.stop()
    st.session_state[has_run_key] = True

    for field in required_fields:
        if values[field] is None or values[field] == "" or values[field] == "\n":
            return None

    return model(**values)
