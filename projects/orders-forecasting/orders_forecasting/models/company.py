from constants.companies import Company
from orders_forecasting.paths import CONFIG_DIR
from orders_forecasting.utils import read_yaml
from pydantic import BaseModel


class CompanyConfig(BaseModel):
    company_name: str
    company_id: str
    language: str
    cut_off_day: int
    min_weeks_per_estm_date: int
    min_year: int
    country: str
    outlier_weeks: list
    train_start: dict


def get_company_config_for_company(company: Company) -> CompanyConfig:
    company_configs = read_yaml(file_name="company_configs", directory=CONFIG_DIR)
    return CompanyConfig(**company_configs[company.company_code])
