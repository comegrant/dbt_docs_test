import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns

from streamlit_app.helpers.utils import apply_filters


st.set_page_config(layout="wide")


def compute_variation(data: pd.DataFrame, column: str) -> pd.DataFrame:
    """Compute variation for a given column"""

    df = data.copy()
    if column not in df.columns:
        return None

    # if column is a list, temporarily explode it
    if df[column].dtype == "object":
        df = df.explode(column)

    valid_data = df[column].fillna("Unknown")
    if valid_data.empty:
        return {
            "total_count": 0,
            "unique_count": 0,
            "missing_count": len(df),
            "diversity_score": 0,
            "distribution": pd.Series([], dtype="int64"),
        }

    # Convert to string to handle mixed types
    valid_data = valid_data.astype(str)
    distribution = valid_data.value_counts()

    return {
        # "total_count": len(data),
        "unique_count": len(distribution),
        "missing_count": df[column].isna().sum(),
        "diversity_score": 5,  # compute this
        "distribution": distribution,
    }


def make_barplot(col: str, new_df: pd.DataFrame, old_df: pd.DataFrame, plot_title: str):
    col_name = col.replace("_", " ").capitalize()
    new_variation = compute_variation(new_df, col)
    old_variation = compute_variation(old_df, col)

    # get all categories present in either distribution
    all_categories = set(new_variation["distribution"].index) | set(
        old_variation["distribution"].index
    )

    # order categories by count in new_df (descending), then by name for ties
    new_counts = new_variation["distribution"].reindex(
        list(all_categories), fill_value=0
    )
    sorted_categories = list(new_counts.sort_values(ascending=False).index)

    # reindex both distributions to the sorted order
    new_values = new_variation["distribution"].reindex(sorted_categories, fill_value=0)
    old_values = old_variation["distribution"].reindex(sorted_categories, fill_value=0)

    fig = go.Figure(
        data=[
            go.Bar(name="New Menu", x=sorted_categories, y=new_values),
            go.Bar(name="Old Menu", x=sorted_categories, y=old_values),
        ]
    )

    fig.update_layout(
        barmode="group",
        xaxis_title=col_name,
        yaxis_title="Count",
        title=plot_title,
        height=400,
        width=800,
        xaxis_tickangle=-45,
        # colorway=["#f7dc6f", "#e74c3c"],
    )

    return fig


def make_donut_plot(
    col: str, new_df: pd.DataFrame, old_df: pd.DataFrame, plot_title: str
):
    new_variation = compute_variation(new_df, col)
    old_variation = compute_variation(old_df, col)

    all_categories = sorted(
        set(new_variation["distribution"].index)
        | set(old_variation["distribution"].index)
    )
    n_colors = len(all_categories)
    colors = sns.color_palette("muted", n_colors=n_colors).as_hex()

    new_values = new_variation["distribution"].reindex(all_categories, fill_value=0)
    old_values = old_variation["distribution"].reindex(all_categories, fill_value=0)

    new_fig = go.Figure()
    new_fig.add_trace(
        go.Pie(
            labels=all_categories,
            values=new_values,
            hole=0.4,
            marker=dict(colors=colors),
        )
    )
    new_fig.update_layout(title_text=f"New Menu: {plot_title}")

    old_fig = go.Figure()
    old_fig.add_trace(
        go.Pie(
            labels=all_categories,
            values=old_values,
            hole=0.4,
            marker=dict(colors=colors),
        )
    )
    old_fig.update_layout(title_text=f"Old Menu: {plot_title}")

    return new_fig, old_fig


def main(df_new, df_old):
    # TAXONOMIES
    st.subheader("Taxonomies")
    tax_fig_new, tax_fig_old = make_donut_plot(
        "taxonomies", df_new, df_old, "Taxonomy Distribution"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(tax_fig_new)
    with col2:
        st.plotly_chart(tax_fig_old)

    # PRICE CATEGORY
    st.subheader("Price Category")
    price_fig_new, price_fig_old = make_donut_plot(
        "price_category_id", df_new, df_old, "Price Category Distribution"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(price_fig_new)
    with col2:
        st.plotly_chart(price_fig_old)

    # COOKING TIME
    st.subheader("Cooking Time")
    time_fig_new, time_fig_old = make_donut_plot(
        "cooking_time_to", df_new, df_old, "Cooking Time Distribution"
    )
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(time_fig_new)
    with col2:
        st.plotly_chart(time_fig_old)

    # MAIN INGREDIENTS
    st.subheader("Main Ingredients")
    main_ingredient_fig_new, main_ingredient_fig_old = make_donut_plot(
        "recipe_main_ingredient_name_local",
        df_new,
        df_old,
        "Main Ingredient Distribution",
    )
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(main_ingredient_fig_new)
    with col2:
        st.plotly_chart(main_ingredient_fig_old)

    st.header("Variation Analysis")
    # TAXONOMIES
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot("taxonomies", df_new, df_old, "Taxonomy Distribution")
        st.plotly_chart(fig)
    with tab2:
        st.write("Drill Down")

    # MAIN INGREDIENTS
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot(
            "recipe_main_ingredient_name_local",
            df_new,
            df_old,
            "Main Ingredient Distribution",
        )
        st.plotly_chart(fig)

    with tab2:
        st.write("Drill Down")

    # MAIN PROTEIN
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot(
            "main_protein",
            df_new,
            df_old,
            "Main Protein Distribution",
        )
        st.plotly_chart(fig)

    with tab2:
        st.write("Drill Down")

    # MAIN CARBS
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot("main_carb", df_new, df_old, "Main Carbs Distribution")
        st.plotly_chart(fig)
    with tab2:
        st.write("Drill Down")

    # CUISINE
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot("cuisine", df_new, df_old, "Cuisine Distribution")
        st.plotly_chart(fig)
    with tab2:
        st.write("Drill Down")

    # DISH TYPE
    tab1, tab2 = st.tabs(["📊 Bar Plot", "🔽 Drill Down"])
    with tab1:
        fig = make_barplot("dish_type", df_new, df_old, "Dish Type Distribution")
        st.plotly_chart(fig)
    with tab2:
        st.write("Drill Down")


if __name__ == "__main__":
    company = st.session_state["company"]
    df_old = st.session_state["old_menu_data"]
    df_new = st.session_state["new_menu_data"]

    st.title(f"Variation Overview for {company}")

    df_new, df_old, _ = apply_filters(df_new, df_old)
    main(df_new, df_old)
