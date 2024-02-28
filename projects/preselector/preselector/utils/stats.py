import pandas as pd


def generate_result_summary(results: list) -> dict:
    # TODO: Create summary statistics of output
    df_results = pd.DataFrame(results)

    return {
        "Number of customers": len(df_results),
        "Number of different flex dishes": 0,
        "Number of dishes not in any baskets": 0,
        "Customers with too few dishes": [],
    }
