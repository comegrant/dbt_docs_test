import pytest
from constants.companies import (
    get_company_by_code,
    get_company_by_id,
    get_company_by_name,
)


@pytest.fixture()
def rt_id() -> str:
    return "5E65A955-7B1A-446C-B24F-CFE576BF52D7"


@pytest.fixture()
def amk_code() -> str:
    return "AMK"


@pytest.fixture()
def gl_name() -> str:
    return "Godtlevert"


def test_get_company_by_id(rt_id: str) -> None:
    test_company = get_company_by_id(rt_id)
    company_name = test_company.company_name
    expected_company_name = "Retnemt"

    assert company_name == expected_company_name


def test_get_company_by_code(amk_code: str) -> None:
    test_company = get_company_by_code(amk_code)
    company_name = test_company.company_name
    expected_company_name = "Adams Matkasse"

    assert company_name == expected_company_name


def test_get_company_by_name(gl_name: str) -> None:
    test_company = get_company_by_name(gl_name)
    company_code = test_company.company_code
    expected_company_code = "GL"

    assert company_code == expected_company_code
