# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false
# pyright: reportReturnType=false

import pandas as pd
import streamlit as st
from project_f.analysis.established import calculate_orders_established
from project_f.misc import override_file
from project_f.paths import DATA_DIR, DATA_DOWNLOADED_DIR


@st.cache_data
def read_established_data(brand: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_established_retention_rate = pd.read_csv(f"{DATA_DIR}/assumptions/established_retention_rate.csv")
    df_established_retention_rate = df_established_retention_rate[
        df_established_retention_rate["company_name"] == brand
    ]

    df_established_customers = pd.read_csv(f"{DATA_DOWNLOADED_DIR}/established_customers.csv")
    df_established_customers = df_established_customers[df_established_customers["company_name"] == brand]
    return (
        df_established_retention_rate,
        df_established_customers,
    )


def section_established(brand: str, budget_end: int) -> tuple[pd.DataFrame, list[dict]]:
    st.header("👨🏻‍🦰 Established customers")
    end_year = budget_end // 100
    end_week = budget_end % 100
    (
        df_established_retention_rate,
        df_established_customers,
    ) = read_established_data(brand=brand)

    st.subheader("💭 Assumptions")
    st.markdown("Share of customers still active at the end of year")
    df_established_retention_rate_edited = st.data_editor(
        df_established_retention_rate, disabled=["company_name", "start_year"]
    )
    if st.button("❗️ Override active customer share file with edited data"):
        file_path = DATA_DIR / "assumptions" / "established_retention_rate.csv"
        override_file(df_new=df_established_retention_rate_edited, original_file_path=file_path)
        st.write(f":red[{file_path} updated!]")

    st.subheader("🤗 :green[Results for already established customers]")
    (
        df_established_final,
        df_established_by_year_per_loyalty,
    ) = calculate_orders_established(
        df_established_customers=df_established_customers,
        df_established_retention_rate=df_established_retention_rate_edited,
        end_year=end_year,
        end_week=end_week,
        num_weeks_compounded=52,
    )
    st.markdown("Established customers by starting year (starting point of order projection)")
    st.dataframe(df_established_by_year_per_loyalty)
    st.markdown("Orders placed by the established customers")
    st.dataframe(df_established_final)
    data_to_save = [
        {
            "sub_folder": "established",
            "file_name": "share_of_active_customers_eoy.csv",
            "dataframe": df_established_retention_rate,
        },
        {
            "sub_folder": "established",
            "file_name": "established_customers_by_start_year.csv",
            "dataframe": df_established_by_year_per_loyalty,
        },
    ]
    return df_established_final, data_to_save
