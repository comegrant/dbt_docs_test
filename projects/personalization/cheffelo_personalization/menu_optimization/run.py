import argparse
import logging
from typing import Any, Dict

from lmkgroup_ds_utils.helpers.naming import camel2snake

from cheffelo_personalization.menu_optimization.optimization import generate_menu
from cheffelo_personalization.menu_optimization.tests.utils import load_test_data
from cheffelo_personalization.utils.constants import (
    DATALAKE_ACCOUNT_ID,
    DATALAKE_CLIENT_ID,
    DATALAKE_CLIENT_SECRET,
    DATALAKE_TENANT_ID,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args():
    parser = argparse.ArgumentParser(description="Run project")
    parser.add_argument(
        "-c",
        "--company",
        default="gl",
        action="store",
        help="Which company to run for",
        type=str,
        required=False,
    )

    parser.add_argument("--save-output-locally", action="store_true")
    parser.add_argument("--write-to-datalake", action="store_true")
    parser.add_argument("--test", action="store_true")

    parser.add_argument(
        "--config",
        default="default",
        action="store",
        help="Which config to run pipeline for",
        type=str,
        required=False,
    )
    parser.add_argument("--local", action="store", default=True, type=bool)
    parser.add_argument(
        "--dl-tenant-id",
        default=DATALAKE_TENANT_ID,
        action="store",
        help="datalake tenant ID",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-client-id",
        default=DATALAKE_CLIENT_ID,
        action="store",
        help="datalake client id",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-client-secret",
        default=DATALAKE_CLIENT_SECRET,
        action="store",
        help="datalake client secret",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--dl-account-url",
        default=DATALAKE_ACCOUNT_ID,
        action="store",
        help="datalake account url",
        type=str,
        required=False,
    )
    args = parser.parse_args()

    return args


def convert_dict_to_snake(dct: Dict[Any, Any] | list[Any]):
    if isinstance(dct, list):
        return [
            convert_dict_to_snake(i) if isinstance(i, (dict, list)) else i for i in dct
        ]

    return {
        camel2snake(a): convert_dict_to_snake(b) if isinstance(b, (dict, list)) else b
        for a, b in dct.items()
    }


def run(
    company: str,
    local: str,
    save_output_locally: bool,
    write_to_datalake: bool,
    dl_tenant_id: str,
    dl_client_id: str,
    dl_client_secret: str,
    dl_account_url: str,
):
    sample_input = load_test_data()
    sample_input = convert_dict_to_snake(sample_input)
    res = generate_menu(
        num_recipes=sample_input["num_recipes"],
        rules=sample_input["rules"],
        week=sample_input["week"],
        year=sample_input["year"],
        company_id=sample_input["company_id"],
        local=True,
    )
    print(res)


def main():
    args = get_args()
    print("Running menu optimization with input {}".format(args))
    run(
        company=args.company,
        local=args.local,
        save_output_locally=args.save_output_locally,
        write_to_datalake=args.write_to_datalake,
        dl_tenant_id=args.dl_tenant_id,
        dl_client_id=args.dl_client_id,
        dl_client_secret=args.dl_client_secret,
        dl_account_url=args.dl_account_url,
    )


if __name__ == "__main__":
    main()
