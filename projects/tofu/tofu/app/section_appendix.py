import pandas as pd
import streamlit as st


def section_appendix(df_future_mapped_with_flex: pd.DataFrame, df_known_mapped_with_flex: pd.DataFrame) -> None:
    st.subheader("📊 Appendix: calendar mapping and projections")
    st.write(df_future_mapped_with_flex.drop(columns=["analysts_forecast_flex_share"]))

    if st.button("Show historical mapping table"):
        df_container = st.empty()
        df_container.write(df_known_mapped_with_flex)

        if st.button("Hide historical mapping table"):
            df_container = st.empty()
