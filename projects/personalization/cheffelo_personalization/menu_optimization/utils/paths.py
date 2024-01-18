import logging
from pathlib import Path
from typing import Optional

from cheffelo_personalization.menu_optimization.utils.constants import RECIPE_BANK_ENV
from cheffelo_personalization.utils.constants import COMPANY_ID_TO_CODE

logger = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).parents[1]
CONFIG_DIR = PROJECT_DIR / "configs"
DATA_DIR = PROJECT_DIR / "data"
SQL_DIR = DATA_DIR / "sql"
OUTPUT_DIR = DATA_DIR / "output"
TEST_DATA_DIR = PROJECT_DIR / "tests" / "data"
LOG_DIR = OUTPUT_DIR / "log"


DATALAKE_URL = Path("https://gganalyticsdatalake.blob.core.windows.net/data-science")
CONTAINER = Path("data-science")
MENU_PLANNING_TEST_URL = Path("test-folder") / "MP20"


def get_recipe_bank_url(company_code: str = "", company_id: str = "") -> Path:
    if not company_code:
        company_code = COMPANY_ID_TO_CODE[company_id.upper()]
    return MENU_PLANNING_TEST_URL / f"PIM_RecipeBank_{company_code}_{RECIPE_BANK_ENV}"


def get_recipe_bank_ingredients_url(
    company_code: str = "", company_id: str = ""
) -> Path:
    if not company_code:
        company_code = COMPANY_ID_TO_CODE[company_id.upper()]
    return (
        MENU_PLANNING_TEST_URL
        / f"PIM_RecipeBankIngredients_{company_code}_{RECIPE_BANK_ENV}"
    )
