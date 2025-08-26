# 🦄 tofu

TOFU = Total Orders Forecast Unicorn 🦄

It's mysterious. It's magical. It knows the future. It knows what the customers want.
It's a unicorn.

TOFU contains a streamlit app that forecasts the total number of orders

# 🚀 Launch the app locally

If this your first time launching the app, you'll need to follow these steps.

Prerequisites:
 - You have cloned the `sous-chef` repo.
 - You have `poetry` installed.

Steps:

1. Initiate the virtual environment and install dependencies:

    ```bash
    cd sous-chef
    cd projects/tofu

    # Make sure you pull the latest changes
    git pull origin main
    poetry env use python3.11
    poetry config in-project true

    # Intiate the virtual environment
    poetry shell
    # check if you have a .venv folder under tofu folder
    ls -la .venv

    # Activate the virtual environment
    source .venv/bin/activate

    # if you have a new poetry version, do
    poetry env activate

    # Install dependencies
    poetry install
    ```
2. Fill in your databricks credentials in the `.env` file.
- if you don't have a `.env` file, create one under the `sous-chef` folder. You can just open a new file and then save it as `.env`, or alternatively in the terminal do:
    ```bash
    cd /{path-to-sous-chef}/sous-chef
    touch .env
    ```
- Open the file and enter these
    ```
    DATABRICKS_HOST=https://adb-<workspacename>azuredatabricks.net
    DATABRICKS_TOKEN=<your-databricks-PAT>
    ```
- if you don't have a `DATABRICKS_TOKEN`, you should create one in the databricks workspace following the instructions [here](https://docs.databricks.com/dev-tools/api/latest/authentication.html). 

3. Launch the app (and for every single time afterwards):

    ```bash
    cd /{path-to-sous-chef}/sous-chef/projects/tofu
    source .venv/bin/activate
    streamlit run projects/tofu/tofu.py
    ```

# 📊 View the app

The app should be running on `http://localhost:8501`. You should automatically see it in a browser.