import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")


def create_constraint_metrics(rule, df_new, df_old):
    """
    Create comprehensive metrics for a business rule constraint
    """
    taxonomy_id = int(rule.taxonomy_id)

    # filter by taxonomy
    df_new_taxonomy = df_new[
        df_new["old_taxonomies"].apply(
            lambda x: isinstance(x, list) and taxonomy_id in x
        )
    ]
    df_old_taxonomy = df_old[
        df_old["old_taxonomies"].apply(
            lambda x: isinstance(x, list) and taxonomy_id in x
        )
    ]

    metrics = {
        "taxonomy_id": taxonomy_id,
        "total_requested": rule.quantity,
        "new_menu_total": len(df_new_taxonomy),
        "old_menu_total": len(df_old_taxonomy),
        "price_categories": [],
        "cooking_times": [],
        "main_ingredients": [],
    }

    # price cat
    for price in rule.price_categories:
        price_category_id = price.price_category_id
        price_category_quantity = price.quantity

        df_new_price = df_new_taxonomy[
            df_new_taxonomy["price_category_id"] == price_category_id
        ]
        df_old_price = df_old_taxonomy[
            df_old_taxonomy["price_category_id"] == price_category_id
        ]

        metrics["price_categories"].append(
            {
                "id": price_category_id,
                "requested": price_category_quantity,
                "new_menu": len(df_new_price),
                "old_menu": len(df_old_price),
                "new_meets_target": len(df_new_price) >= price_category_quantity,
                "old_meets_target": len(df_old_price) >= price_category_quantity,
            }
        )

    # cooking time
    for time in rule.cooking_times:
        cooking_time_to = time.cooking_time_to
        cooking_time_quantity = time.quantity

        df_new_time = df_new_taxonomy[
            df_new_taxonomy["cooking_time_to"] <= cooking_time_to
        ]
        df_old_time = df_old_taxonomy[
            df_old_taxonomy["cooking_time_to"] <= cooking_time_to
        ]

        metrics["cooking_times"].append(
            {
                "max_time": cooking_time_to,
                "requested": cooking_time_quantity,
                "new_menu": len(df_new_time),
                "old_menu": len(df_old_time),
                "new_meets_target": len(df_new_time) >= cooking_time_quantity,
                "old_meets_target": len(df_old_time) >= cooking_time_quantity,
            }
        )

    # main ingredient
    for main_ingredient in rule.main_ingredients:
        main_ingredient_id = main_ingredient.main_ingredient_id
        main_ingredient_quantity = main_ingredient.quantity

        df_new_main_ingredient = df_new_taxonomy[
            df_new_taxonomy["main_ingredient_id"] == main_ingredient_id
        ]
        df_old_main_ingredient = df_old_taxonomy[
            df_old_taxonomy["main_ingredient_id"] == main_ingredient_id
        ]

        metrics["main_ingredients"].append(
            {
                "id": main_ingredient_id,
                "requested": main_ingredient_quantity,
                "new_menu": len(df_new_main_ingredient),
                "old_menu": len(df_old_main_ingredient),
                "new_meets_target": len(df_new_main_ingredient)
                >= main_ingredient_quantity,
                "old_meets_target": len(df_old_main_ingredient)
                >= main_ingredient_quantity,
            }
        )

    return metrics


def create_summary_table(metrics):
    """
    Create a summary table for the constraint analysis
    """
    summary_data = []

    # taxonomy summary
    summary_data.append(
        {
            "Type": "Taxonomy",
            "ID": f"{metrics['taxonomy_id']}",
            "Required": metrics["total_requested"],
            "New Menu": metrics["new_menu_total"],
            "Old Menu": metrics["old_menu_total"],
            "New Gap": metrics["new_menu_total"] - metrics["total_requested"],
            "Old Gap": metrics["old_menu_total"] - metrics["total_requested"],
            "New Meets Target": metrics["new_menu_total"] >= metrics["total_requested"],
            "Old Meets Target": metrics["old_menu_total"] >= metrics["total_requested"],
            # "New Compliance": metrics["new_menu_total"] >= metrics["total_requested"],
            # "Old Compliance": metrics["old_menu_total"] >= metrics["total_requested"],
        }
    )

    # price
    for price in metrics["price_categories"]:
        summary_data.append(
            {
                "Type": "Price Category",
                "ID": f"{price['id']}",
                "Required": price["requested"],
                "New Menu": price["new_menu"],
                "Old Menu": price["old_menu"],
                "New Gap": price["new_menu"] - price["requested"],
                "Old Gap": price["old_menu"] - price["requested"],
                "New Meets Target": price["new_meets_target"],
                "Old Meets Target": price["old_meets_target"],
                # "New Compliance": price["new_menu"] >= price["requested"],
                # "Old Compliance": price["old_menu"] >= price["requested"],
            }
        )

    # cooking times
    for time in metrics["cooking_times"]:
        summary_data.append(
            {
                "Type": "Cooking Time",
                "ID": f"≤{time['max_time']} min",
                "Required": time["requested"],
                "New Menu": time["new_menu"],
                "Old Menu": time["old_menu"],
                "New Gap": time["new_menu"] - time["requested"],
                "Old Gap": time["old_menu"] - time["requested"],
                "New Meets Target": time["new_meets_target"],
                "Old Meets Target": time["old_meets_target"],
                # "New Compliance": time["new_menu"] >= time["requested"],
                # "Old Compliance": time["old_menu"] >= time["requested"],
            }
        )

    # main ingredients
    for ingredient in metrics["main_ingredients"]:
        summary_data.append(
            {
                "Type": "Main Ingredient",
                "ID": f"{ingredient['id']}",
                "Required": ingredient["requested"],
                "New Menu": ingredient["new_menu"],
                "Old Menu": ingredient["old_menu"],
                "New Gap": ingredient["new_menu"] - ingredient["requested"],
                "Old Gap": ingredient["old_menu"] - ingredient["requested"],
                "New Meets Target": ingredient["new_meets_target"],
                "Old Meets Target": ingredient["old_meets_target"],
                # "New Compliance": ingredient["new_menu"] >= ingredient["requested"],
                # "Old Compliance": ingredient["old_menu"] >= ingredient["requested"],
            }
        )

    return pd.DataFrame(summary_data)


def display_constraint_analysis(rule, df_new, df_old):
    """
    Main function to display the constraint analysis for a single rule
    """
    taxonomy_id = int(rule.taxonomy_id)

    metrics = create_constraint_metrics(rule, df_new, df_old)

    st.subheader(f"Taxonomy {taxonomy_id}")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="Taxonomy Recipes Required", value=metrics["total_requested"])

    with col2:
        st.metric(
            label="New Menu Available",
            value=metrics["new_menu_total"],
            delta=metrics["new_menu_total"] - metrics["total_requested"],
        )

    with col3:
        st.metric(
            label="Old Menu Available",
            value=metrics["old_menu_total"],
            delta=metrics["old_menu_total"] - metrics["total_requested"],
        )

    with col4:
        new_compliance = metrics["new_menu_total"] >= metrics["total_requested"]
        old_compliance = metrics["old_menu_total"] >= metrics["total_requested"]

        if new_compliance and old_compliance:
            status = "✅ Both Compliant"
        elif new_compliance:
            status = "⚠️ New Only"
        elif old_compliance:
            status = "⚠️ Old Only"
        else:
            status = "❌ Neither"

        st.metric(label="Status", value=status)

    tab1, tab2, tab3 = st.tabs(
        ["📋 Summary Table", "🔽 Detailed Breakdown", "📈 Visual Analysis"]
    )

    with tab1:
        # st.markdown("#### Compliance Summary")
        summary_df = create_summary_table(metrics)

        # style
        styled_df = summary_df.style.format().background_gradient(
            subset=["New Gap", "Old Gap"],
            cmap="YlOrRd_r",
            vmax=0,
            vmin=-5,  # TODO: decide if better color exists an vmin lim
        )

        st.dataframe(styled_df, use_container_width=True)

    with tab2:
        st.subheader("Detailed Constraint Breakdown")

        # price
        if metrics["price_categories"]:
            st.write("**Price Category Constraints:**")
            for price in metrics["price_categories"]:
                with st.expander(f"Price Category {price['id']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Required:** {price['requested']}")
                        st.write(
                            f"**New Menu:** {price['new_menu']} {'✅' if price['new_meets_target'] else '❌'}"
                        )
                        st.write(
                            f"**Old Menu:** {price['old_menu']} {'✅' if price['old_meets_target'] else '❌'}"
                        )
                    with col2:
                        gap_new = price["new_menu"] - price["requested"]
                        gap_old = price["old_menu"] - price["requested"]
                        st.write(f"**New Menu Gap:** {gap_new:+d}")
                        st.write(f"**Old Menu Gap:** {gap_old:+d}")

        # cooking times
        if metrics["cooking_times"]:
            st.write("**Cooking Time Constraints:**")
            for time in metrics["cooking_times"]:
                with st.expander(f"Cooking Time ≤{time['max_time']} minutes"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Required:** {time['requested']}")
                        st.write(
                            f"**New Menu:** {time['new_menu']} {'✅' if time['new_meets_target'] else '❌'}"
                        )
                        st.write(
                            f"**Old Menu:** {time['old_menu']} {'✅' if time['old_meets_target'] else '❌'}"
                        )
                    with col2:
                        gap_new = time["new_menu"] - time["requested"]
                        gap_old = time["old_menu"] - time["requested"]
                        st.write(f"**New Menu Gap:** {gap_new:+d}")
                        st.write(f"**Old Menu Gap:** {gap_old:+d}")

        # main ingredients
        if metrics["main_ingredients"]:
            st.write("**Main Ingredient Constraints:**")
            for ingredient in metrics["main_ingredients"]:
                with st.expander(f"Main Ingredient {ingredient['id']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Required:** {ingredient['requested']}")
                        st.write(
                            f"**New Menu:** {ingredient['new_menu']} {'✅' if ingredient['new_meets_target'] else '❌'}"
                        )
                        st.write(
                            f"**Old Menu:** {ingredient['old_menu']} {'✅' if ingredient['old_meets_target'] else '❌'}"
                        )
                    with col2:
                        gap_new = ingredient["new_menu"] - ingredient["requested"]
                        gap_old = ingredient["old_menu"] - ingredient["requested"]
                        st.write(f"**New Menu Gap:** {gap_new:+d}")
                        st.write(f"**Old Menu Gap:** {gap_old:+d}")

    with tab3:
        # TODO: create visuals
        st.write("Visual Analysis")


def main(business_rules, df_new, df_old):
    """
    Main loop to process all business rules
    """

    # overall statistics
    st.info(f"Status: {st.session_state['status']}")
    st.info(f"Error: {st.session_state['error']}")

    st.header("Overall Analysis")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.write("")
    with col2:
        st.metric("New Menu Recipes", len(df_new))
    with col3:
        st.metric("Old Menu Recipes", len(df_old))
    with col4:
        st.write("")

    # business rule breakdown
    for _, rule in enumerate(business_rules):
        st.markdown("---")
        display_constraint_analysis(rule, df_new, df_old)


if __name__ == "__main__":
    company = st.session_state["company"]
    df_old = st.session_state["old_menu_data"]
    df_new = st.session_state["new_menu_data"]
    payload = st.session_state["payload"]

    st.title(f"Target Overview for {company}")
    st.write("Comparing how well old and new menus satisfy business constraints")

    main(
        payload.companies[0].taxonomies,
        df_new,
        df_old,
    )
