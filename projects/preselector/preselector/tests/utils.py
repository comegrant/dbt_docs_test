import json

from preselector.utils.paths import TEST_DATA_DIR


def load_test_data() -> tuple:
    # Load data from json
    agreements = json.loads((TEST_DATA_DIR / "agreements.json").read_text())
    product_information = json.loads((TEST_DATA_DIR / "product_information.json").read_text())
    menu_products = json.loads((TEST_DATA_DIR / "menu_products.json").read_text())
    recommendation_scores = json.loads((TEST_DATA_DIR / "recommendation_scores.json").read_text())
    preference_rules = json.loads((TEST_DATA_DIR / "product_preference_rules.json").read_text())

    return (
        agreements,
        product_information,
        menu_products,
        recommendation_scores,
        preference_rules,
    )
