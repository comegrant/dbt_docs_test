import json
import os.path as osp

from cheffelo_personalization.basket_preselector.utils.paths import TEST_DATA_DIR


def open_file(fn):
    with open(fn) as f:
        data = json.load(f)
    return data


def load_test_data():
    # Load data from json
    agreements = open_file(osp.join(TEST_DATA_DIR, "agreements.json"))
    product_information = open_file(osp.join(TEST_DATA_DIR, "product_information.json"))
    menu_products = open_file(osp.join(TEST_DATA_DIR, "menu_products.json"))
    recommendation_scores = open_file(
        osp.join(TEST_DATA_DIR, "recommendation_scores.json")
    )
    preference_rules = open_file(
        osp.join(TEST_DATA_DIR, "product_preference_rules.json")
    )

    return (
        agreements,
        product_information,
        menu_products,
        recommendation_scores,
        preference_rules,
    )
