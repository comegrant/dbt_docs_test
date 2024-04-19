import pytest
from constants.product_types import (
    PRODUCT_TYPE_ID_MEALBOXES,
    get_product_type_id,
)


@pytest.fixture()
def a_valid_product_type() -> str:
    return "mealbox"


def test_get_product_type_id(a_valid_product_type: str) -> None:
    prod_id_result = get_product_type_id(a_valid_product_type)
    expected = PRODUCT_TYPE_ID_MEALBOXES

    assert prod_id_result == expected
