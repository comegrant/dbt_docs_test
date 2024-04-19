# Constants

A python package that contains the commonly used constants at Cheffelo

## Install
Run the following command to install this package:

```bash
chef add constants
```

## Example usage
### company constants

1. You simply want to get a company_id and other properties of the company you know, you can import the company object directly

    ```
    from constants.companies imoprt company_amk
    print(company_amk.company_id)
    ```

2. You want to get all properties of a specific company, for example AMK, through the company code

    ```
    from constants.companies import get_company_by_code

    company_amk = get_company_by_code("AMK")
    print(company_amk.company_id)
    ```

3. You can also get company properties by their name, or their company_id through:
    ```
    from constants.companies import get_company_by_name, get_company_by_id

    # It's case insensitive, and it's possible to only pass in part of the name
    company_lmk = get_company_by_name("Linas")

    # This is Linas
    company_id = "6A2D0B60-84D6-4830-9945-58D518D27AC2"

    company = get_company_by_id(company_id)
    company_code = company.company_code

### product types constants
1. Use the constants directly
    ```
    from constants.product_types import PRODUCT_TYPE_ID_DISHES, PRODUCT_TYPE_MEALBOXES, PRODUCT_TYPE_ID_GROCERIES
    ```

2. You want to get the product type id
    ```
    from constants.product_types import get_product_type_id
    groceries_type_id = get_product_type_id("groceries")
    mealboxes_type_id = get_product_type_id("mealboxes")
    ```

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.


### Testing
Run the following command to test that the changes do not impact critical functionality:

```bash
chef test
```
This will spin up a Docker image and run your code in a similar environment as our production code.

### Linting
We have added linting which automatically enforces coding style guides.
You may as a result get some failing pipelines from this.

Run the following command to fix and reproduce the linting errors:

```bash
chef lint
```

You can also install `pre-commit` if you want linting to be applied on commit.
