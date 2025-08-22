from catalog_connector import connection
from attribute_scoring.train.configs import DataConfig
from attribute_scoring.common import Args
from constants.companies import get_company_by_code
from pathlib import Path
from attribute_scoring.paths import TRAIN_SQL_DIR

DATA_CONFIG = DataConfig()


def get_training_data(args: Args):
    company_id = get_company_by_code(args.company).company_id
    sql_path = Path(TRAIN_SQL_DIR) / "training_targets.sql"

    with sql_path.open() as f:
        query = f.read().format(company_id=company_id)

    df = connection.sql(query).toPandas()

    return df
