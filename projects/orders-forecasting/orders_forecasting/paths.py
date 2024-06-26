from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
SQL_DIR = PROJECT_DIR / "sqls"
CONFIG_DIR = PROJECT_DIR / "configs"
PROJECT_NAME = str(PROJECT_DIR).split("/")[-1]
