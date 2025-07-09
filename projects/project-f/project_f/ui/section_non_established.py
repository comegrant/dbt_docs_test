# pyright: reportAttributeAccessIssue=false
# pyright: reportArgumentType=false


import numpy as np
import pandas as pd
import streamlit as st
from project_f.analysis.non_established import (
    calculate_general_cohort,
    calculate_orders,
    get_historical_acquisition_all_segment,
    get_start_delivery,
    keep_latest_cohorts,
    summarize_non_established_customers,
)
from project_f.misc import override_file
from project_f.paths import DATA_DIR, DATA_DOWNLOADED_DIR
from project_f.visualisation import plot_cohort, plot_onboarding_avg_orders


def section_non_established(brand: str) -> None:
    st.header("👶🏻 Non established customers")
    (
        df_new_customers,
        df_reactivated_customers,
        df_source_effect,
        df_weeks_until_start,
        df_marketing_input,
        df_calendar,
    ) = read_data(brand)

    df_historical_acquisition = get_historical_acquisition_all_segment(
        df_new_customers=df_new_customers,
        df_reactivated_customers=df_reactivated_customers,
        df_calendar=df_calendar,
    )
    st.subheader("💹 Marketing target")
    col1, col2 = st.columns(2)
    with col1:
        st.write("Marketing target")
        df_marketing_input_edited = st.data_editor(
            df_marketing_input,
            disabled=["company_name", "year", "month", "source", "customer_segment"],
        )
        if st.button("❗️ Override marketing input file with edited data"):
            # TODO: replace as csv at some point
            file_path = DATA_DOWNLOADED_DIR / "marketing_input.csv"
            override_file(df_new=df_marketing_input_edited, original_file_path=file_path)
            st.write(f":red[{file_path} updated!]")
    with col2:
        st.write("Historical customer acquisition")
        unique_year_months = np.sort(
            (df_historical_acquisition["year"] * 100 + df_historical_acquisition["month"]).unique()
        )[::-1]
        col2_1, col2_2 = st.columns(2)
        with col2_1:
            start_year_month = st.selectbox(
                "minimum year-month to add",
                unique_year_months,
                key="start_year_month",
                index=6,
            )

        with col2_2:
            end_year_month = st.selectbox(
                "max year-month to add",
                unique_year_months,
                key="end_year_month",
                index=0,
            )
        df_historical_acquisition_selected = df_historical_acquisition[
            ((df_historical_acquisition["year"] * 100 + df_historical_acquisition["month"]) >= start_year_month)
            & ((df_historical_acquisition["year"] * 100 + df_historical_acquisition["month"]) <= end_year_month)
        ]
        st.dataframe(df_historical_acquisition_selected)
    df_historical_acquisition_selected_to_add = df_historical_acquisition_selected
    df_historical_acquisition_selected_to_add["budget_type"] = df_marketing_input["budget_type"].unique()[0]
    df_historical_acquisition_selected_to_add = df_historical_acquisition_selected_to_add[df_marketing_input.columns]
    st.write("Marketing input data combined with historical acquisition:")
    df_marketing_input_edited_combined = pd.concat(
        [df_historical_acquisition_selected_to_add, df_marketing_input_edited],  # type: ignore
        ignore_index=True,  # type: ignore
    )  # type: ignore

    df_marketing_input_edited_combined = df_marketing_input_edited_combined.sort_values(by=["year", "month"])
    st.dataframe(df_marketing_input_edited_combined)

    st.subheader("💭 Assumptions")
    col1, col2 = st.columns((0.5, 0.5))
    with col1:
        st.write("Assumed first week of delivery")
        df_weeks_until_start_edited = st.data_editor(
            df_weeks_until_start, disabled=["company_name", "source", "weeks_til_start"]
        )
        if st.button("❗️ Override first delivery week file with edited data"):
            file_path = DATA_DIR / "assumptions" / "weeks_until_start.csv"
            override_file(df_new=df_weeks_until_start_edited, original_file_path=file_path)
            st.write(f":red[{file_path} updated!]")

    with col2:
        st.write("Assumed sales channel's effect on cohort")
        df_source_effect_edited = st.data_editor(df_source_effect, disabled=["source"])
        if st.button("❗️ Override sales channel effect file with edited data"):
            file_path = DATA_DIR / "assumptions" / "source_effect.csv"
            override_file(df_new=df_source_effect_edited, original_file_path=file_path)
            st.write(f":red[{file_path} updated!]")

    # To be displayed on the UI
    df_cohorts = calculate_general_cohort(
        df_new_customers=df_new_customers, df_reactivated_customers=df_reactivated_customers, df_calendar=df_calendar
    )
    st.subheader("👯 Cohort Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.write("Selected cohorts (editable)")
        cohort_file = DATA_DIR / "assumptions/latest_cohorts.csv"
        if not cohort_file.exists():
            df_cohort_latest = keep_latest_cohorts(df_cohorts=df_cohorts, num_months=12)
        else:
            df_cohort_latest = pd.read_csv(cohort_file)
            if brand not in df_cohort_latest["company_name"].unique():
                df_cohort_latest = keep_latest_cohorts(df_cohorts=df_cohorts, num_months=12)
            else:
                df_cohort_latest = df_cohort_latest[df_cohort_latest["company_name"] == brand]

        df_cohort_latest_edited = st.data_editor(
            df_cohort_latest,
            disabled=[
                "company_name",
                "year",
                "month",
                "method_code",
                "customer_segment",
                "cohort_week",
            ],
        )
        col1_1, col1_2 = st.columns(2)
        with col1_1:
            if st.button("❗️ Override latest cohort with adjusted cohort"):
                override_file(df_new=df_cohort_latest_edited, original_file_path=cohort_file)  # type: ignore
                st.write(f":red[{cohort_file} updated!]")
        with col1_2:
            if st.button("🎨 Plot selected cohort"):
                st.session_state["is_plot_cohort"] = True
    with col2:
        df_cohorts_filtered = df_cohorts
        st.write("Reference: historical cohort calculations")
        col2_1, col2_2, col2_3, col2_4 = st.columns(4)
        with col2_1:
            customer_segment_hist = st.selectbox(
                "customer segment",
                (
                    "all",
                    "New Customers",
                    "Reactivated Customers",
                    "Reactivated Customers No Code",
                ),
                key="customer_segment_hist",
            )
            if customer_segment_hist != "all":
                df_cohorts_filtered = df_cohorts_filtered[
                    df_cohorts_filtered["customer_segment"] == customer_segment_hist
                ]
        with col2_2:
            cohort_year = st.selectbox(
                "year",
                ("all", "2021", "2022", "2023", "2024", "2025"),
                key="cohort_year_hist",
            )
            if cohort_year != "all":
                df_cohorts_filtered = df_cohorts_filtered[df_cohorts_filtered["year"].astype(str) == cohort_year]
        with col2_3:
            cohort_month_hist = st.selectbox(
                "month",
                ("all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
                key="cohort_month_hist",
            )
            if cohort_month_hist != "all":
                df_cohorts_filtered = df_cohorts_filtered[df_cohorts_filtered["month"].astype(str) == cohort_month_hist]
        with col2_4:
            cohort_week_hist = st.selectbox(
                "cohort week",
                ("all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
                key="cohort_week_hist",
            )
            if cohort_week_hist != "all":
                df_cohorts_filtered = df_cohorts_filtered[
                    df_cohorts_filtered["cohort_week"].astype(str) == cohort_week_hist
                ]
        st.write(df_cohorts_filtered)

    if st.session_state["is_plot_cohort"]:
        df_cohort_latest_to_plot = df_cohort_latest_edited
        col1, col2 = st.columns(2)
        with col1:
            customer_segment = st.selectbox(
                "customer_segment",
                (
                    "all",
                    "New Customers",
                    "Reactivated Customers",
                    "Reactivated Customers No Code",
                ),
            )
            if customer_segment != "all":
                df_cohort_latest_to_plot = df_cohort_latest_to_plot[
                    df_cohort_latest_to_plot["customer_segment"] == customer_segment
                ]
        with col2:
            month = st.selectbox(
                "month",
                ("all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"),
            )
            if month != "all":
                df_cohort_latest_to_plot = df_cohort_latest_to_plot[
                    df_cohort_latest_to_plot["month"].astype(str) == month
                ]
        fig_cohorts = plot_cohort(df_cohort_latest_to_plot=df_cohort_latest_to_plot, brand=brand)  # type: ignore
        st.plotly_chart(fig_cohorts)

    df_started_delivery = get_start_delivery(
        df_marketing_input=df_marketing_input_edited_combined,
        df_weeks_until_start=df_weeks_until_start_edited,
        df_calendar=df_calendar,
    )
    df_started_delivery_merged, df_non_established_final = calculate_orders(
        df_cohort_latest=df_cohort_latest_edited,
        df_started_delivery=df_started_delivery,
        df_source_effect=df_source_effect_edited,
    )

    st.subheader("🤗 :green[Results for non-established customers]")

    st.markdown("Monthly cohort summary")
    df_cohort_latest_monthly_summary = pd.DataFrame(
        df_cohort_latest_edited.groupby(["company_name", "year", "month", "customer_segment"])["average_orders"].sum()
    ).reset_index()
    st.dataframe(df_cohort_latest_monthly_summary)

    st.markdown("Number of orders placed by non-established customers")
    st.dataframe(df_non_established_final)

    st.markdown("8 weeks average orders per customer")
    df_ne_summary = summarize_non_established_customers(
        df_marketing_input=df_marketing_input_edited,
        df_non_established_orders=df_non_established_final,
    )
    st.dataframe(df_ne_summary)
    fig_8w_avg = plot_onboarding_avg_orders(df_ne_summary=df_ne_summary)
    st.plotly_chart(fig_8w_avg)

    data_to_save = [
        {
            "sub_folder": "non_established",
            "file_name": "assumed_first_week_of_delivery.csv",
            "dataframe": df_weeks_until_start,
        },
        {
            "sub_folder": "non_established",
            "file_name": "sales_channel_effect.csv",
            "dataframe": df_source_effect_edited,
        },
        {
            "sub_folder": "non_established",
            "file_name": "selected_cohorts.csv",
            "dataframe": df_cohort_latest_edited,
        },
        {
            "sub_folder": "non_established",
            "file_name": "monthly_cohort_summary.csv",
            "dataframe": df_cohort_latest_monthly_summary,
        },
        {
            "sub_folder": "non_established",
            "file_name": "12_week_avg_orders.csv",
            "dataframe": df_ne_summary,
        },
    ]

    return (  # type: ignore
        df_source_effect_edited,
        df_cohort_latest_edited,
        df_started_delivery,
        df_non_established_final,
        df_started_delivery_merged,
        data_to_save,
    )


@st.cache_data
def read_data(brand: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_new_customers = pd.read_csv(f"{DATA_DOWNLOADED_DIR}/new_customers.csv")
    df_new_customers = df_new_customers[df_new_customers["company_name"] == brand]

    df_reactivated_customers = pd.read_csv(f"{DATA_DOWNLOADED_DIR}/reactivated_customers.csv")
    df_reactivated_customers = df_reactivated_customers[df_reactivated_customers["company_name"] == brand]

    df_source_effect = pd.read_csv(DATA_DIR / "assumptions" / "source_effect.csv")
    df_source_effect = df_source_effect[df_source_effect["company_name"] == brand]

    df_weeks_until_start = pd.read_csv(DATA_DIR / "assumptions/weeks_until_start.csv")
    df_weeks_until_start = df_weeks_until_start[df_weeks_until_start["company_name"] == brand]

    df_marketing_input = pd.read_csv(
        f"{DATA_DOWNLOADED_DIR}/marketing_input.csv",
    )
    # Remove empty spaces in the source column.
    # I.e., Sales - Phone will become Sales-Phone
    df_marketing_input["source"] = df_marketing_input["source"].str.replace(" ", "")
    df_marketing_input["customer_segment"] = df_marketing_input["customer_segment"].str.title()
    df_marketing_input = df_marketing_input[df_marketing_input["company_name"] == brand]

    df_calendar = pd.read_csv(
        f"{DATA_DOWNLOADED_DIR}/budget_calendar.csv",
    )

    return (  # type: ignore
        df_new_customers,
        df_reactivated_customers,
        df_source_effect,
        df_weeks_until_start,
        df_marketing_input,
        df_calendar,
    )
