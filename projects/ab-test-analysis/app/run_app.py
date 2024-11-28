import streamlit as st
from app.session_states import init_session_states
from app.section_get_data import section_get_data_from_sql
from app.section_analysis import section_analysis


def main() -> None:
    # Set Streamlit page configuration
    st.set_page_config(
        page_title="A/B Test Analysis",
        layout="wide",  # Use the full screen width
        initial_sidebar_state="expanded"
    )
    # Title of the app
    st.title("🧪 A/B Test Analysis")
    init_session_states()

    df_data = section_get_data_from_sql()
    section_analysis(df_data=df_data)


if __name__ == "__main__":
    main()
