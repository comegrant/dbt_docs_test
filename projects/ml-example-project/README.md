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
    - `mlflow`: our preferred MLOps tool, works seamlessly work Databricks🧱
    - `ipykernel`: most data scientists probably knows this one 😅
    - `scikit-learn`: as famous as Jupyter notebooks so needs no introduction
    - [`databricks-connect`](https://pypi.org/project/databricks-connect/): the package that can let you connect to databricks workspace from VS code. Make sure to pin a version of the databricks-connect package that is the same version as the [databricks runtime](https://docs.databricks.com/en/release-notes/runtime/index.html)
    - `pydantic`: we often use pydantic models to define run arguments
    - `pytest`: thy shall write tests 😅

5. Install the dependencies using `poetry install` or `chef install`. Please take a look at your project folder and make sure you have both the `poetry.lock` and `pyproject.toml` file.


<!-- poetry shell, poetry install, make sure that lock file is created
- have databricks.yml for DABs
- make sure docker image version is consistent with project python requirement -->
## The ML project structure

When building a typical ML project, we recommend following the folder structure proposed in the [DS Conventions](https://chef.getoutline.com/doc/ds-conventions-rSlHTt5P4L) document. In general, the `ml-project` has a main module in snakecase `ml_project`, and there are two main folders under it: the `train` and `predict`folders. Other main folders are: `sandbox`, `jobs`.

Disclaimer: the content below provides general guideline, but it does not need to be followed religiously.

### The `train` folder
- `data.py`: contains code that pulls data from the data warehouse and creates training set
- `model.py`: the file that contains the model definition
- `train.py`: the file that contains the main training procedure, where functions like `get_training_set`, `model.fit()` etc are called, and ends at logging and/or registering a model in the databricks catalog.
- `configs.py`: the file where you specify configs related to train. Examples are configs for creating [feature lookups](https://api-docs.databricks.com/python/feature-store/latest/feature_store.feature_lookup.html), model hyperparameters, constants, etc

### The `predict` folder
- `data.py`: contains code that pulls data from the data warehouse and create the dataset for prediction
- `predict.py`: contains code that loads a registered model, load the predict dataset, performs prediction and write outputs
- `configs.py`: contains configs that are required for the prediction step; such as constants and registered models' uris

### The `jobs` folder
This is the folder that contains databricks notebooks as well as the DABs yaml files, which is to be deployed to and run on the databricks workspace. Typically, this folder contains:
- `train.py`: a databricks notebook that calls the train function from `project-name/project_name/train/train.py`
- `train.yml`: the workflow definition related to the ``project-name/jobs/train.py` notebook
- `predict.py`: a databricks notebook that calls the train function from `project-name/project_name/predict/predict.py`
- `predict.yml`: the workflow definition related to the ``project-name/jobs/predict.py` notebook

### The `sandbox` folder
Keep your adhoc `.ipynb` jupyter notebooks and/or whatever stuff you do not want quality assurance here. However, stripping your jupyter notebooks outputs before you commit and push is a good practice.

### The `test` folder
WIP: Stuff related to writing tests
