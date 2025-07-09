from pathlib import Path

import streamlit as st
from project_f.paths import DATA_DIR


def save_output_data(brand: str, data_dict_list: list[dict]) -> None:
    """data_dict_list should be of format:
    [
        {
            "sub_folder": folder_name,
            "file_name": file_name,
            "dataframe": df
        }
    ]

    """
    output_folder_name = "outputs"
    output_folder_path = DATA_DIR / output_folder_name
    if not output_folder_path.exists():
        Path.mkdir(output_folder_path)
        st.write(f"Folder {output_folder_path} created \n")

    brand_output_folder_path = output_folder_path / brand
    if not brand_output_folder_path.exists():
        Path.mkdir(brand_output_folder_path)
        st.write(f"Folder {brand_output_folder_path} created \n")

    for data_dict in data_dict_list:
        sub_folder_name = data_dict["sub_folder"]
        sub_folder_path = brand_output_folder_path / sub_folder_name
        if not sub_folder_path.exists():
            Path.mkdir(sub_folder_path)
            st.write(f"Folder {sub_folder_path} created \n")

        file_name = data_dict["file_name"]
        data_dict["dataframe"].to_csv(Path(sub_folder_path / file_name), index=False)

        st.markdown(f"File :blue[{file_name}] saved in {sub_folder_path} \n")
