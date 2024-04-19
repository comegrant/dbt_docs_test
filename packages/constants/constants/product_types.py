PRODUCT_TYPE_ID_DISHES = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
PRODUCT_TYPE_ID_MEALBOXES = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"
PRODUCT_TYPE_ID_GROCERIES = "ADB241C3-DECA-4FD7-A173-923BE1A7ABD3"


def get_product_type_id(product_type: str) -> str:
    product_type_lower_no_space = product_type.lower().replace(" ", "")
    if product_type_lower_no_space in [
        "dishes",
        "vv",
        "velg&vrag",
        "velgogvrag",
        "standalonedishes",
        "dish",
        "standalonedish",
    ]:
        return PRODUCT_TYPE_ID_DISHES
    elif product_type_lower_no_space in ["mealboxes", "mealbox"]:
        return PRODUCT_TYPE_ID_MEALBOXES
    elif product_type_lower_no_space in [
        "standalonegrocery",
        "standalonegroceries",
        "groceries",
        "grocery",
    ]:
        return PRODUCT_TYPE_ID_GROCERIES
    else:
        raise ValueError(f"{product_type} is not a recognized product type name.")
