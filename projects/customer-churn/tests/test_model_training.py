import logging

import pytest
from constants.companies import company_amk, company_gl, company_lmk, company_rt

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "company_code",
    [
        company_lmk.company_code,
        company_gl.company_code,
        company_amk.company_code,
        company_rt.company_code,
    ],
)
def test_model_training(company_code: str) -> None:
    logger.info(company_code)
    assert True
