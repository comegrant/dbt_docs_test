import asyncio
import logging
from pathlib import Path

import pandas as pd
from data_contracts.sources import azure_dl_creds

# load correct ENV
from dotenv import find_dotenv, load_dotenv
from menu_optimiser.utils.constants import mapping

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)


async def input_recipes(company_id: str) -> pd.DataFrame:
    company = ""

    # Get the value from the mapping
    company = mapping.get(company_id)
    logger.info(f"Analyzing: {company_id} with abbreviation: {company}")

    df_recipes = (
        # await data_science_data_lake.directory("test-folder/MP20/")
        await azure_dl_creds.directory("data-science")
        .directory("test-folder/MP20/")
        .parquet_at(f"PIM_RecipeBank_{company}_PRD")
        .to_pandas()  # we need pandas as the rest of the processing uses pandas methods like isin etc
    )

    return df_recipes


async def main() -> None:
    company_to_test = "5e65a955-7b1a-446c-b24f-cfe576bf52d7"  # "8a613c15-35e4-471f-91cc-972f933331d7"
    # "6a2d0b60-84d6-4830-9945-58d518d27ac2"
    outdir = "./data_api"
    outfile = "input_recipes"
    Path(outdir).mkdir(parents=True, exist_ok=True)
    results = await input_recipes(company_id=company_to_test)
    results.to_csv(f"{outdir}/{outfile}_{company_to_test}_.csv")
    # print(results.head())

    await input_recipes(company_to_test)


if __name__ == "__main__":
    asyncio.run(main())
