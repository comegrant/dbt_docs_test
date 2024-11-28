import streamlit as st
import pandas as pd
from ab_test_analysis.analysis import get_hypothesis_test, get_summary
from ab_test_analysis.visualisation import plot_histogram


def section_analysis(df_data: pd.DataFrame) -> None:
    if st.session_state["has_data"]:
        st.divider()
        st.header("🧐 Analysis")
        test_type = "T-test (default)"
        additional_metrics = None
        is_advanced = st.toggle("Advanced options")
        colnames = df_data.columns
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            response_colname = st.selectbox(
                "What is the column that contains the response variable?", colnames
            )
        with col2:
            group_colname = st.selectbox(
                "What is the column that indicate group assignments?", colnames
            )
            st.session_state["is_indicated_group"] = True
        if st.session_state["is_indicated_group"]:
            unique_group_names = df_data[group_colname].unique()
            with col3:
                test_group_name = st.selectbox(
                    "What is the name of the test group?", unique_group_names
                )
                st.session_state["is_indicated_group"] = True
        with col4:
            alternative = st.selectbox(
                "What kind of test do you want to perform?",
                ("two-sided", "greater", "less")
            )
        with col5:
            alpha = st.selectbox(
                "What is the significance level (alpha)?",
                (0.05, 0.1, 0.02, 0.01)
            )
        if is_advanced:
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                test_type = st.selectbox(
                    "Choose a statistical test",
                    ("T-test (default)", "Mann-Whitney U Test")
                )
            with col2:
                additional_metrics = st.multiselect(
                    "Additional summary statistics",
                    ["Cohen's d"],
                    ["Cohen's d"]
                )
        if st.button("Run analysis"):
            st.session_state["ready_to_analyze"] = True

    if st.session_state["ready_to_analyze"]:
        df_hypothesis_test, cohen_d_result = get_hypothesis_test(
            df_ab_test=df_data,
            test_group_name=test_group_name,
            group_colname=group_colname,
            response_colname=response_colname,
            alternative_hypothesis=alternative,
            test_type=test_type,
        )
        pval = df_hypothesis_test["p-value"].values[0]
        conclude_experiment(pval=pval, alternative_hypothesis=alternative, alpha=alpha)
        col1, col2, col3 = st.columns(3)
        with col1:
            st.write("Experiment data summary")
            df_summary = get_summary(
                df_ab_test=df_data,
                group_colname=group_colname,
                response_colname=response_colname
            )
            st.write(df_summary)
        with col2:
            st.write("Test statistics")
            st.dataframe(df_hypothesis_test)
        if additional_metrics is not None:
            additional_metrics_dict = {}
            with col3:
                st.write("Additional metrics:")
                if "Cohen's d" in additional_metrics:
                    additional_metrics_dict["Cohen's d"] = [cohen_d_result]
                else:
                    additional_metrics_dict["Cohen's d"] = [None]
                df_additional_metrics = pd.DataFrame(additional_metrics_dict).transpose()
                df_additional_metrics.columns = ["information"]
                st.write(df_additional_metrics)

        fig_histogram = plot_histogram(
            df_ab_test=df_data,
            group_colname=group_colname,
            response_colname=response_colname
        )
        st.plotly_chart(fig_histogram)
        # st.write(ttest_result)


def conclude_experiment(
    pval: float,
    alpha: float,
    alternative_hypothesis: str
) -> None:
    st.header("Conclusion")
    if alternative_hypothesis == "two-sided":
        key_word = "different from"
    else:
        key_word = alternative_hypothesis + " than "

    if pval < alpha:
        st.write(f"😱 There is statistically significant evidence \
                 to indicate that test group's mean is {key_word} control group!")
    else:
        st.write(f"🤷 Not enough evidence to indicate that \
                 test group's mean is significantly {key_word} control group's")
