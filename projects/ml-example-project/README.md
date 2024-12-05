# 🤖 ML Example Project

A simple template that walks you through the A-Z of a typical machine learning project at Cheffelo.

## 🏁 Getting started with a new ML project
0. Make sure you have `poetry` and `chef-cli` installed and set up, as well as the desired python version for your project. If unsure of which version of python you are on, run `python --version` in your terminal
1. Create a new project folder using the chef CLI using``chef create project``, and follow all steps as indicated

2. Initiate a local poetry environment using the command ``poetry shell``. If you have configured poetry's [``virtualenvs.in-project``](https://python-poetry.org/docs/configuration/#virtualenvsin-project) variable to be true, you should see a `.venv` folder being created under the `sous-chef/projects/{your-project-name}/` folder.

3. Activate the virtual environment using

    3a. if you are on a Mac or Linux machine:
    ```
    source .venv/bin/activate
    ```
    3b. if you are on a Windows machine:
    ```
    source .venv/Scripts/activate
    ```
4. Optional: add some necessary dependencies using the command `chef add {your-desired-dependencies}`. This step is optional as you can always add more along the way as you develop the project. However, some common ones that might be useful:

    - `constants`: an internal package that allows you to conveniently retrieve brand and product properties such as `company_id`, `product_type_id`, etc
    - `slack-connector`: an internal package that allows you to send notifications to slack to notify workflow's success/failure
    - `databricks-env`: an internal package that let you set up the environment variables that we use for databricks notebooks
    - `mlflow`: our preferred MLOps tool, works seamlessly work Databricks
    - `ipykernel`: most data scientists probably knows this one 😅
    - `scikit-learn`: as famous as Jupyter notebooks so needs no introduction
    - [`databricks-connect`](https://pypi.org/project/databricks-connect/): the package that can let you connect to databricks workspace from VS code. Make sure to pin a version of the databricks-connect package that is the same version as the [databricks runtime](https://docs.databricks.com/en/release-notes/runtime/index.html)
    - `pydantic`: we often use pydantic models to define run arguments
    - `pytest`: thy shall write tests 😅

5. Install the dependencies using `poetry install` or `chef install`. Please take a look at your project folder and make sure you have both the `poetry.lock` and `pyproject.toml` file.


<!-- poetry shell, poetry install, make sure that lock file is created
- have databricks.yml for DABs
- make sure docker image version is consistent with project python requirement -->
