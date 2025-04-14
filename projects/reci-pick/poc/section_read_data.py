import pandas as pd
import streamlit as st
from reci_pick.paths import PROJECT_DIR


@st.cache_data()
def read_data(company_code: str) -> pd.DataFrame:
    data_path = PROJECT_DIR / "data"
    rec_data_path = data_path / "recommendations"
    filenames_to_read = list(rec_data_path.glob(f"{company_code}*.csv"))

    rec_file_list = []
    for filename in filenames_to_read:
        file_path = rec_data_path / filename
        df_rec = pd.read_csv(file_path)
        rec_file_list.append(df_rec)
    df_full = pd.concat(rec_file_list)
    df_recipe = pd.read_csv(data_path / "recipe_photos_url.csv")
    df_recipe = df_recipe.drop_duplicates(subset="main_recipe_id", keep="last")
    df_full = df_full.merge(df_recipe)
    return df_full
