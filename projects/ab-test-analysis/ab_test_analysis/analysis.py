import numpy as np
import pandas as pd
from scipy import stats
import streamlit as st


def get_summary(
    df_ab_test: pd.DataFrame,
    group_colname: str,
    response_colname: str,
) -> pd.DataFrame:
    df_summary = (
        df_ab_test
        .groupby(group_colname)[response_colname]
        .aggregate(["mean", "median", "std", "count"])
        .reset_index()
    )
    return df_summary


def get_hypothesis_test(
    df_ab_test: pd.DataFrame,
    group_colname: str,
    response_colname: str,
    alternative_hypothesis: str | None = "two-sided",
    test_group_name: str | None = "",
    control_group_name: str | None = "",
    test_type: str | None = "T-test (default)",
) -> pd.DataFrame:
    """Conduct a t-test to test the two independent samples.

    Args:
        df_ab_test (pd.DataFrame): The dataframe of your A/B test, consists at least of:
            - a column specifies the group or variant
            - a column specifying the response
            - usually also a column specifying the ID. See below for an example:
                id  | group    | has_purchase
                123 | test     | 0
                234 | test     | 1
                345 | control  | 1
                456 | test     | 0
        group_colname (str): the column name of the group column
        response_colname (str): the column name of the response column
        test_group_name (Optional[str]): the group name of the test group.
            Not needed if conducting two-sided test.
        control_group_name (Optional[str]): the group name of the control group.
            Not needed if conducting two-sided test.
        alternative_hypothesis (Optional[str], optional): the alternative hypothesis.
            valid values: ["two-sided", "greater", "less"].
            Defaults to "two-sided".
            - "two-sided" : if you want to test that the mean of the two groups are different
                NB: the null hypothesis is then H_0: mean(test) = mean(control)
            - "greater": if you want to test that mean(test) > mean(control).
                i.e., null hypothesis is mean(test) <= mean(control), we run a one tail test
            - "less": if you want to test that mean(test) < mean(control).
                i.e., null hypothesis is mean(test) >= mean(control)

    Returns:
        stats._result_classes.TtestResult
    """
    # Split the results dataframe to be two dataframes
    # df_a = test
    # df_b = control
    expected_num_groups = 2
    nunique_group_names = df_ab_test[group_colname].nunique()
    # Check if the number of unique values in the group column is 2
    assert nunique_group_names == expected_num_groups, \
        f"Expected {expected_num_groups} unique values in '{group_colname}', but got {nunique_group_names}"
    if (test_group_name == "") and (control_group_name == ""):
        if alternative_hypothesis != "two-sided":
            raise ValueError("Please specify test and control group names")
        else:
            test_group_name, control_group_name = df_ab_test[group_colname].unique()

    df_test, df_control = split_test_control_groups(
        df_ab_test=df_ab_test,
        group_colname=group_colname,
        test_group_name=test_group_name,
        control_group_name=control_group_name
    )

    response_test = df_test[response_colname].astype(float)
    response_control = df_control[response_colname].astype(float)
    if test_type == "T-test (default)":
        df_test_statistics = run_ttest(
            response_test=response_test,
            response_control=response_control,
            alternative_hypothesis=alternative_hypothesis
        )
    elif test_type == "Mann-Whitney U Test":
        df_test_statistics = run_mwu_test(
            response_test=response_test,
            response_control=response_control,
            alternative_hypothesis=alternative_hypothesis
        )
    cohen_d_result = get_cohen_d_result(
        response_test=response_test,
        response_control=response_control,
    )
    return df_test_statistics, cohen_d_result


def split_test_control_groups(
    df_ab_test: pd.DataFrame,
    group_colname: str,
    test_group_name: str | None = "",
    control_group_name: str | None = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split result df to be two: one for test and one for control

    Args:
        df_ab_test (pd.DataFrame): a/b test dataframe
        group_colname (str): name of the column specifying which group each
            response belongs to
        test_group_name (Optional[str], optional): the test group's name.
            Defaults to "".
        control_group_name (Optional[str], optional): the control group's name.
            Defaults to "".
            NB: at least one of [test_group_name, control_group_name] must be specified.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: two dataframes, corresponds to test and control group.
    """
    # Only support two independent samples test at the moment
    if test_group_name is not None:
        df_test = df_ab_test[df_ab_test[group_colname] == test_group_name]
        df_control = df_ab_test[df_ab_test[group_colname] != test_group_name]
    elif control_group_name is not None:
        df_test = df_ab_test[df_ab_test[group_colname] != control_group_name]
        df_control = df_ab_test[df_ab_test[group_colname] == control_group_name]
    return df_test, df_control


def run_ttest(
    response_test: pd.Series,
    response_control: pd.Series,
    alternative_hypothesis: str,
) -> pd.DataFrame:
    # To decide whether the equal variance assumption can be used
    var_ratio = max(
        np.var(response_test)/np.var(response_control),
        np.var(response_control)/np.var(response_test)
    )
    ratio_threshold = 4   # 4 is the rule of thumb
    is_equal_var = (var_ratio < ratio_threshold)
    t_test_results = stats.ttest_ind(
        a=response_test,
        b=response_control,
        equal_var=is_equal_var,
        alternative=alternative_hypothesis,
    )
    df_t_test = pd.DataFrame(t_test_results).transpose()
    df_t_test.columns = ["t_statistic", "p-value"]
    return df_t_test


def run_mwu_test(
    response_test: pd.Series,
    response_control: pd.Series,
    alternative_hypothesis: str,
) -> pd.DataFrame:
    if alternative_hypothesis != "two-sided":
        st.write(":red[Warning: Mann-Whitney test is by default two-sided, \
                 please choose T-test if you intend to run a one-tailed test!]")
    mwu_test_result = stats.mannwhitneyu(
        response_test, response_control
    )
    df_mwu_test_result = pd.DataFrame(mwu_test_result).transpose()
    df_mwu_test_result.columns = ["U statistic", "p-value"]
    return df_mwu_test_result


def get_cohen_d_result(
    response_control: pd.Series,
    response_test: pd.Series,
) -> str:
    cohen_d_value = compute_cohen_d(
        response_control=response_control,
        response_test=response_test,
    )
    effect_size = get_cohen_d_explanation(
        d=cohen_d_value
    )
    result_str = f"d = {cohen_d_value:.3f}, \
        implying that effect size is {effect_size}"
    return result_str


def compute_cohen_d(
    response_test: pd.Series,
    response_control: pd.Series
) -> float:
    """reference:
    https://medium.com/@riteshgupta.ai/a-b-testing-with-python-a-complete-guide-a042224ee1bb

    Args:
        response_test (pd.Series)
        response_control (pd.Series)

    Returns:
        float: Cohen's d
    """
    diff = response_test.mean() - response_control.mean()
    pooled_sd = np.sqrt(
        (
            (len(response_control) - 1) * response_control.var() + (len(response_test) - 1) * response_test.var()
        ) / (len(response_control) + len(response_test) - 2)
    )
    return diff / pooled_sd


def get_cohen_d_explanation(
    d: float
) -> str:
    """Refernce:
    https://en.wikipedia.org/wiki/Effect_size#Cohen's_d

    Args:
        d (float): Cohen's d

    Returns:
        str: ["very small", "small", "medium", "large", "very large", "huge"]
    """
    abs_d = abs(d)
    effect_size = ""
    if abs_d < 0.01: # noqa
        effect_size = "very small"
    elif abs_d < 0.2: # noqa
        effect_size = "small"
    elif abs_d < 0.5: # noqa
        effect_size = "medium"
    elif abs_d < 0.8: # noqa
        effect_size = "large"
    elif abs_d < 1.2: # noqa
        effect_size = "very large"
    else:
        effect_size = "huge"
    return effect_size
