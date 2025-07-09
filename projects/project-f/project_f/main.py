# pyright: reportGeneralTypeIssues=false
# pyright: reportPossiblyUnboundVariable=false

import asyncio
import logging
from pathlib import Path

import streamlit as st

from project_f.paths import DATA_DOWNLOADED_DIR
from project_f.ui.section_appendix import section_appendix
from project_f.ui.section_download_data import section_download_data
from project_f.ui.section_established import section_established
from project_f.ui.section_newly_established import section_newly_established
from project_f.ui.section_non_established import section_non_established
from project_f.ui.section_save_outputs import save_output_data
from project_f.ui.section_set_params import section_set_params
from project_f.ui.section_summary import section_summary

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Project F @ NDP", layout="wide")


async def main() -> None:
    st.title("Welcome to Project F 💰")
    budget_type, budget_year, budget_start, budget_end, brand = section_set_params()
    st.divider()

    st.header("🔢 Have you downloaded the data needed?")
    if "has_data" not in st.session_state:
        st.session_state["has_data"] = False

    if "is_plot_cohort" not in st.session_state:
        st.session_state["is_plot_cohort"] = False

    if "replaced_with_forecast" not in st.session_state:
        st.session_state["replaced_with_forecast"] = False

    if not DATA_DOWNLOADED_DIR.exists():
        Path.mkdir(DATA_DOWNLOADED_DIR)
        st.write(f"Created a directory {DATA_DOWNLOADED_DIR} to save downloaded data.")

    if st.button("I need to download/refresh the data from DB"):
        section_download_data(budget_start=budget_start, budget_year=budget_year, budget_type=budget_type)
        st.session_state["has_data"] = True

    if st.button("I have downloaded data before"):
        st.write("Please proceed to the next section!")
        st.session_state["has_data"] = True
    if st.session_state["has_data"]:
        st.divider()
        (
            df_source_effect_edited,
            df_cohort_latest_edited,
            df_started_delivery,
            df_non_established_final,
            df_started_delivery_merged,
            data_to_save_non_established,
        ) = section_non_established(brand=brand)

        st.divider()
        (
            df_orders_newly_established_agg,
            data_to_save_newly_established,
        ) = section_newly_established(
            df_source_effect=df_source_effect_edited,
            df_cohort_latest=df_cohort_latest_edited,
            df_started_delivery=df_started_delivery,
            df_started_delivery_merged=df_started_delivery_merged,
            brand=brand,
            budget_end=budget_end,
        )

        st.divider()
        df_established_final, data_to_save_established = section_established(brand=brand, budget_end=budget_end)

        st.divider()
        df_summary_merged, data_to_save_model_outputs = section_summary(
            df_newly_established=df_orders_newly_established_agg,
            df_non_established=df_non_established_final,
            df_established=df_established_final,
            budget_start=budget_start,
            budget_end=budget_end,
            brand=brand,
        )

        st.header("Appendix")
        data_to_save_appendix = section_appendix(
            brand=brand,
            df_summary_merged=df_summary_merged,
            budget_start=budget_start,
            budget_type=budget_type,
        )

        if st.button("Save output data"):
            save_output_data(
                brand=brand,
                data_dict_list=data_to_save_non_established
                + data_to_save_newly_established
                + data_to_save_established
                + data_to_save_model_outputs
                + data_to_save_appendix,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
