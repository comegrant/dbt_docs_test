from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
MODULE_DIR = PROJECT_DIR / "attribute_scoring"
TRAIN_SQL_DIR = MODULE_DIR / "train" / "sqls"
PREDICT_SQL_DIR = MODULE_DIR / "predict" / "sqls"
