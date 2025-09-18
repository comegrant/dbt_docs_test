# pyright: reportCallIssue=false

import pandas as pd
import streamlit as st


def section_appendix(df_known_mapped_with_flex: pd.DataFrame) -> None:
    st.subheader("📊 Appendix: calendar mapping and projections")
    with st.expander("**Click here to see mapping history** 👈"):
        st.dataframe(
            df_known_mapped_with_flex[
                [
                    "is_need_special_attention",
                    "menu_year",
                    "menu_week",
                    "mapped_menu_year",
                    "mapped_menu_week",
                    "holiday_list",
                    "mapped_holiday_list",
                    "is_around_easter",
                    "is_around_easter_mapped_week",
                    "num_holidays",
                    "num_holidays_mapped",
                    "is_holiday_overlap_with_weekend",
                    "num_holidays_effectively",
                    "num_holidays_effectively_mapped",
                    "total_orders",
                    "mapped_total_orders",
                    "flex_share",
                    "mapped_flex_share",
                ]
            ].rename(
                columns={
                    "mapped_menu_year": "menu_year_mapped",
                    "mapped_menu_week": "menu_week_mapped",
                    "mapped_holiday_list": "holiday_list_mapped",
                    "is_around_easter_mapped_week": "is_around_easter_mapped",
                    "mapped_num_holidays": "num_holidays_mapped",
                    "mapped_num_holidays_effectively": "num_holidays_effectively_mapped",
                    "mapped_flex_share": "flex_share_mapped",
                }
            )[::-1],
            hide_index=True,
        )
