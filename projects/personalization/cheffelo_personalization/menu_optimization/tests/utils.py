import json
import os.path as osp

from cheffelo_personalization.menu_optimization.utils.paths import TEST_DATA_DIR


def open_file(fn):
    with open(fn) as f:
        data = json.load(f)
    return data


def load_test_data():
    # Load data from json
    example_input = open_file(osp.join(TEST_DATA_DIR, "example_input.json"))
    return example_input
