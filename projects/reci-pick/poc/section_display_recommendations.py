import pandas as pd
import streamlit as st


def section_display_recommendations(
    df_recommendations: pd.DataFrame,
    num_image_per_row: int = 5,
) -> None:
    yyyywws = df_recommendations["menu_yyyyww"].unique()
    for yyyyww in yyyywws:
        df_one_week = df_recommendations[df_recommendations["menu_yyyyww"] == yyyyww]
        year = df_one_week["menu_year"].values[0]
        week = df_one_week["menu_week"].values[0]
        st.subheader(f"Menu week {year} - week {week}")
        display_image(
            urls=df_one_week["full_path"], texts=df_one_week["recipe_name"], num_image_per_row=num_image_per_row
        )


def display_image(
    urls: list[str],
    texts: list[str],
    num_image_per_row: int = 5,
) -> None:
    cols = st.columns(num_image_per_row)  # Create 5 columns
    for i, url in enumerate(urls):
        with cols[i % num_image_per_row]:  # Use the corresponding column for each image, cycling through columns
            st.image(url, use_container_width=True)
            st.write(texts.iloc[i])
        if (i + 1) % num_image_per_row == 0:  # After every 5 images, create a new row
            cols = st.columns(num_image_per_row)  # Reset columns for the next row
