from pathlib import Path

PROJECT_DIR = Path(__file__).parents[1]
CONFIG_DIR = PROJECT_DIR / "configs"
DATA_DIR = PROJECT_DIR / "data"
SQL_DIR = DATA_DIR / "sql"
SQL_CUSTOMER_DIR = SQL_DIR / "customer"
SQL_PRODUCT_DIR = SQL_DIR / "products"
SQL_REC_ENGINE_DIR = SQL_DIR / "rec_engine"
OUTPUT_DIR = DATA_DIR / "output"
TEST_DATA_DIR = PROJECT_DIR / "tests" / "data"
LOG_DIR = OUTPUT_DIR / "log"
