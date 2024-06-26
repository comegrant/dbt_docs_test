from pathlib import Path

PROJECT_DIR = Path(__file__).parent
SQL_DIR = PROJECT_DIR / "data" / "sqls"
CONFIG_DIR = PROJECT_DIR.parent / "configs"
DATABRICKS_DATA_DIR = Path("/dbfs/customer_churn/data")
DATA_DIR = PROJECT_DIR.parent / "data"
MODEL_DIR = PROJECT_DIR.parent / "models"
OUTPUT_DIR = DATA_DIR / "results"

INTERIM_DATA_DIR = DATA_DIR / "interim"
DATA_PROCESSED_DIR = DATA_DIR / "processed"
