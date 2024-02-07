from types import UnionType
from pydantic import BaseModel, Field
from typing import Callable, Any, get_args, TypeVar
from pydantic.fields import FieldInfo
import streamlit as st

T = TypeVar("T", bound=BaseModel)

def list_input(name: str, info: FieldInfo) -> list[str] | None:
    if isinstance(info.default, list):
        value = ",".join(info.default)
    else:
        value = info.default

    values = st.text_input(name, value=value)
    st.caption(f"Comma separated list of {name}")
    if not values:
        return None
    return str(values).split(",")


streamlit_components: dict[str, Callable[[str, FieldInfo], Any]] = {
    "str": lambda name, info: st.text_input(name, value=info.default),
    "list": lambda name, info: list_input(name, info),
    "int": lambda name, info: st.number_input(name, value=info.default),
    "Optional": lambda name, info: st.text_input(name),
}


def pydantic_form(key: str, model: type[T]) -> T | None:

    values = {}
    required_fields = set()

    with st.form(key=key):
        for name, field in model.model_fields.items():
            annotation = field.annotation
            is_optional = False

            if isinstance(annotation, UnionType):
                sub_types = list(get_args(annotation))

                if len(sub_types) == 2 and type(None) in sub_types:
                    is_optional = True
                    annotation = sub_types[0] if sub_types[0] != type(None) else sub_types[1]


            type_name = annotation.__name__
            component = streamlit_components[type_name]


            if not is_optional:
                required_fields.add(name)
                values[name] = component(f"{name} *", field)
            else:
                values[name] = component(name, field)

        st.form_submit_button()

    for field in required_fields:
        if not values[field] or values[field] == "" or values[field] == "\n":
            return None

    return model(**values)
