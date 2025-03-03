<h1 align="center">
🧑‍🍳 Sous-chef 🧑‍🍳
</h1>

<p align="center">
<img src="assets/sous-chef.png" alt="sous-chef" width="200"/>
</p>

<p align="center">
Sous chef is the Python monorepo for the data team.
</p>

## Table of Contents

- [Setting Up Your Machine](#setting-up-your-machine) 🛠️
- [Creating a new project or package](#creating-a-new-project-or-package) 🆕
  - [What is the difference between \<library name\>, \<package name\> and \<module name\>?](#what-is-the-difference-between-library-name-package-name-and-module-name)
  - [Use Cases](#use-cases) 💡
    - [Python Project](#python-project)
    - [Data Science](#data-science)

## Setting Up Your Machine 🛠️
This guide will help you set up your machine with the necessary tools to start developing with `sous-chef`. It will go through the following steps:

- Python and `pyenv` installation
- Poetry installation
- Git installation
- `sous-chef` cloning
- `sous-chef` CLI installation

### 1. Setting up GitHub
Start by making sure your GitHub account is set up correctly.

1. Configure two-factor authentication in your GitHub [profile settings](https://github.com/settings/security).
    - We recommend using the GitHib Mobile app ([iOS](https://apps.apple.com/us/app/github/id1477376905) / [Android](https://play.google.com/store/apps/details?id=com.github.android&hl=en)) for this, but you can also use another authentication method.
2. If your GitHub account isn't set up with your `cheffelo.com` email address, then you need to add it in your [profile settings](https://github.com/settings/emails).

### 2. Installing VSCode and relevant extensions
You need a code editor to work on the code. We recommend using either VSCode or Cursor (which is an AI powered code editor built on VSCode).

<details>
<summary>Cursor</summary>

The benefit of cursor is that it is very good at understanding our code base, offers great autocomplete, and you can ask it questions about the code base.

1. Go to https://www.cursor.com and install the version for your machine.
2. Follow the on-screen prompts to complete the installation
  - Keyboard: Default
  - Language: Up to you
  - Codebase-wide: Enable
  - Add terminal command: Install cursor
  - If you are currently using VSCode, it will ask you if you want to import your VSCode settings, click yes if you want Cursor to act like your current VSCode setup
  - Automplete preference: Contiunue with default
  - Data preferences: Privacy mode
  - Sign up and login using GitHub

3. We will now install a few extensions to help with development. Open up a terminal in Cursor and run the following:
  ```bash
  code --install-extension innoverio.vscode-dbt-power-user && \
  code --install-extension databricks.databricks && \
  code --install-extension analysis-services.TMDL && \
  code --install-extension GerhardBrueckl.powerbi-vscode && \
  code --install-extension jianfajun.dax-language && \
  code --install-extension ms-python.python && \
  code --install-extension samuelcolvin.jinjahtml && \
  code --install-extension redhat.vscode-yaml && \
  code --install-extension sdras.night-owl
  ```
4. We installed some theme options, to change theme options, press `Ctrl (or cmd) + Shift + P` and select `Preferences: Color Theme` and choose your theme.

</details>

<details>
<summary>VSCode</summary>

1. Go to https://code.visualstudio.com/download and install the version for your machine.
2. We will now install a few extensions to help with development. Open up a terminal in VSCode and run the following:
  ```bash
  code --install-extension innoverio.vscode-dbt-power-user && \
  code --install-extension databricks.databricks && \
  code --install-extension analysis-services.TMDL && \
  code --install-extension GerhardBrueckl.powerbi-vscode && \
  code --install-extension jianfajun.dax-language && \
  code --install-extension ms-python.python && \
  code --install-extension samuelcolvin.jinjahtml && \
  code --install-extension redhat.vscode-yaml && \
  code --install-extension sdras.night-owl
  ```
3. We installed some theme options, to change theme options, press `Ctrl (or cmd) + Shift + P` and select `Preferences: Color Theme` and choose your theme.

</details>

### 3. Install Python
_A lot of the steps from here onwards will require copying commands into your terminal, so we recommend opening up your code editor on one half of your screen, and this readme on the other half._

Python is the back-bone of `sous-chef`, so we need to install this first. But each project can have different Python versions. Therefore, we will also install `pyenv` to manage different Python versions on your local machine.

<details>
<summary>Windows</summary>

1. Open your code editor and the built-in terminal with Powershell.
2. Install pyenv-win.
```powershell
Invoke-WebRequest -UseBasicParsing -Uri "https://raw.githubusercontent.com/pyenv-win/pyenv-win/master/pyenv-win/install-pyenv-win.ps1" -OutFile "./install-pyenv-win.ps1"; &"./install-pyenv-win.ps1"
```

If you get a "Running scripts is disabled on this system" error, you can enable it by running the following:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned
```

3. Add pyenv to your path

Adding PYENV, PYENV_HOME and PYENV_ROOT to your Environment Variables:
```powershell
[System.Environment]::SetEnvironmentVariable('PYENV',$env:USERPROFILE + "\.pyenv\pyenv-win\","User")

[System.Environment]::SetEnvironmentVariable('PYENV_ROOT',$env:USERPROFILE + "\.pyenv\pyenv-win\","User")

[System.Environment]::SetEnvironmentVariable('PYENV_HOME',$env:USERPROFILE + "\.pyenv\pyenv-win\","User")
```

Now adding the following paths to your USER PATH variable in order to access the pyenv command:
```powershell
[System.Environment]::SetEnvironmentVariable('path', $env:USERPROFILE + "\.pyenv\pyenv-win\bin;" + $env:USERPROFILE + "\.pyenv\pyenv-win\shims;" + [System.Environment]::GetEnvironmentVariable('path', "User"),"User")
```

If for some reason you cannot execute PowerShell command, type "environment variables for you account" in Windows search bar and open Environment Variables dialog.

You will need create those 3 new variables in System Variables section (bottom half). Let's assume username is `my_pc`.

|Variable|Value|
|---|---|
|PYENV|C:\Users\my_pc\\.pyenv\pyenv-win\
|PYENV_HOME|C:\Users\my_pc\\.pyenv\pyenv-win\
|PYENV_ROOT|C:\Users\my_pc\\.pyenv\pyenv-win\

And add two more lines to user variable `Path`.
```
C:\Users\my_pc\.pyenv\pyenv-win\bin
C:\Users\my_pc\.pyenv\pyenv-win\shims
```

4. Close and reopen your code editor

5. Check if the installation was successful.
```
pyenv --version
```

6. Check a list of Python versions supported by `pyenv-win`
```
pyenv install -l
```

7. Install python 3.11
```
pyenv install 3.11.5
```

8. Install python 3.10
```
pyenv install 3.10.11
```

9. Set a Python version as the global version

```
pyenv global 3.11.5
```

10. Check which Python version you are using and its path

```
pyenv version
```
Output: `<version> (set by \path\to\.pyenv\pyenv-win\.python-version)`

11. Check that Python is working

```
python -c "import sys; print(sys.executable)"
```
Output: `\path\to\.pyenv\pyenv-win\versions\<version>\python.exe`

12. Install pip such that we can install packages later
```
python -m ensurepip --upgrade
```

13. Check that pip is working
```
pip --version
```

</details>

<details>
<summary>macOS</summary>

We will install `pyenv` and Python using Homebrew.

1. Open your code editor and the built-in terminal.
1. Install Homebrew if you haven't already
  ```bash
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  ```

2. Use Homebrew to install `pyenv`
  ```bash
    brew update
    brew install pyenv
  ```

3. Check that `pyenv` is installed correctly by running
```bash
pyenv --version
```

_It should return something like `pyenv 2.X.X`_

4. Add the following to your `.zshrc` file, this will enable `pyenv` in your terminal
```bash
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

Then close your terminal and open a new one.

5. Install Xcode command line tools
```bash
xcode-select --install
```

6. Install `pyenv` dependencies
```bash
brew install openssl readline sqlite3 xz zlib tcl-tk
```

7. Check that `pyenv` is in your path
```bash
which pyenv
```

8. Check that `pyenv`'s shims directory is in your path
```bash
echo $PATH | grep --color=auto "$(pyenv root)/shims"
```

9. Install Python 3.11 using `pyenv`
```bash
pyenv install 3.11.5
```

10. Install Python 3.10 using `pyenv`
```bash
pyenv install 3.10.15
```

11. Set the global Python version to 3.11
```bash
pyenv global 3.11.5
```

12. Check that `pyenv`has versions available
```bash
pyenv versions
```

</details>

#### Check Python installation
To check that you have set up Python correctly, run the following command:
```bash
python --version
```

It should return something like `Python 3.11.X`

### 4. Install Poetry
Poetry is a tool for dependency management in Python projects. It helps manage project dependencies, virtual environments, and package publishing.

<details>
<summary>Windows</summary>

1. Install Poetry using the official installer in PowerShell
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

2. Add Poetry to your PATH by
```powershell
[System.Environment]::SetEnvironmentVariable(
    "path",
    [System.Environment]::GetEnvironmentVariable("path", "User") + ";%APPDATA%\Python\Scripts",
    "User"
)
```

Restart your code editor

3. Check that you have set up Poetry correctly
```bash
poetry --version
```

_It should return something like `Poetry version 1.8.X`_

If you get an `access denied` error, then you may need to add `%APPDATA%\Python\Scripts` to the exemptions:
- Go to Settings > Security > Virus & threat protection
- Under Virus & threat protection settings select Manage settings
- Under Exclusions select Add or remove exclusions
- Select Add an exclusion
- Choose Folder
- Locate and add your Python/Scripts folder (e.g. `C:\Users\<your-username>\AppData\Local\Programs\Python\Scripts`)

4. Set poetry to prefer the currently active Python version

```bash
poetry config virtualenvs.prefer-active-python true
poetry config virtualenvs.in-project true
```

Note: running `poetry self update`on Windows may be problematic. If so, run a re-install of Poetry by running the following:
```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

</details>

<details>
<summary>macOS</summary>

1. Install Poetry using the official installer
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Add poetry to your path
```bash
echo 'export PATH="$HOME/.poetry/bin:$PATH"' >> ~/.zshrc
```

3. Check that you have set up Poetry correctly
```bash
poetry --version
```

_It should return something like `Poetry version 1.8.X`_

4. Set poetry to prefer the currently active Python version and create project specific virtualenvs

```bash
poetry config virtualenvs.prefer-active-python true
poetry config virtualenvs.in-project true
```

</details>

### 5. Install Git
Git is a version control system that allows you to track changes to your code. It is essential for managing and collaborating on projects.

<details>
<summary>Windows</summary>

1. Visit https://git-scm.com/download/win
2. Download and run the Git installer for Windows.
3. There will be a lot of prompts to choose between. Choose the default options apart from the follows:
- Default branch name suffix: `main`
- Default editor: your code editor
4. Repoen your code editor
5. Open command prompt and check that Git is installed correctly by running:
```
git --version
```
_It should return something like `git version 2.X.X`_

5. Install GitHub CLI
```
winget install --id github.cli
```
_Accept the source agreements when prompted_

</details>

<details>
<summary>macOS</summary>

1. Install Git
```
brew install git
```
2. Install GitHub CLI
```
brew install gh
```

</details>

#### Configuring git
Once git is installed, we should provide git with our full name and email address.

1. Start by setting your full name.

```
git config --global user.name "Your Full Name"
```

2. Set the email address, this should be the email address you use to access GitHub.

```
git config --global user.email "Your GitHub Email"
```

3. Set the pull strategy to only fast-forward.

_We recommend requiring git to only fast-forward when pulling, instead of trying (and potentially failing) to rebase. This will stop git from putting itself in a bad state._
```
git config --global pull.ff only
```

4. Check that you have set up git correctly:
```
git --version
```
_It should return something like `git version 2.X.X`_

#### Authenticate with GitHub

Before cloning the repository, you need to authenticate your local machine with GitHub. This step ensures that you have the necessary permissions to access the repository.

1. Authenticate with GitHub
   ```
   gh auth login
   ```

2. Follow the prompts to complete the authentication process using the following answers
  - What account do you want to log into? - GitHub.com
  - What is your preferred protocol for Git operations? - HTTPS
  - Authenticate Git with your GitHub credentials? - Yes
  - How would you like to authenticate GitHub CLI? - Login with a web browser

### 6. Clone Repo
Now let's get the sous-chef repo cloned to your local machine.

We recommend creating a directory within your home directory named `cheffelo` and place all of Cheffelo's source code repositories in there.

1. Navigate to your home/user directory

`cd %USERPROFILE%` (Windows) `cd ~` (macOS)

2. Create a cheffelo directory
```
mkdir cheffelo
```

2. CD into the cheffelo directory
```
cd cheffelo
```

3. Clone the sous-chef repo
```
git clone https://github.com/cheffelo/sous-chef sous-chef
```

4. CD into the sous-chef directory
```
cd sous-chef
```

#### Windows: Adding the folder to avoid access issues on
There is an issue on Windows where "Windows Defender" blocks the access to the sous-chef folder by default. To fix this, we need to add the folder to the "Exclusions", to do this:

1. Go to Settings > Security > Virus & threat protection
2. Under Virus & threat protection settings select Manage settings
3. Under Exclusions select Add or remove exclusions
4. Select Add an exclusion
5. Choose Folder
6. Add the sous-chef folder

### 7. Installing pre-commit hooks
We use pre-commit hooks to ensure code quality and consistency across the monorepo.

<details>
<summary>Windows</summary>

1. Install pre-commit
```
pip install pre-commit
```
If pip is not installed, then go to the `Install Python` section to install this

2. Install the pre-commit hooks
```
cd cheffelo/sous-chef
pre-commit install
```

3. Update the pre-commit hooks
```
pre-commit autoupdate
```

</details>

<details>
<summary>macOS</summary>

1. Install pre-commit
```
brew install pre-commit
```
2. Install the pre-commit hooks
```
cd cheffelo/sous-chef
pre-commit install
```

3. Update the pre-commit hooks
```
pre-commit autoupdate
```

</details>

### 8. Activate the `chef` cli
Now we have the sous-chef report cloned. Let's install the dependencies and activate the `chef` cli.

Run the following commands to install the dependencies and activate the `chef` cli.

```bash
cd cheffelo/sous-chef
poetry shell
poetry install
```

This will spin up a new Python virtualenv, and activate the venv in a new shell.
It will also install the core utils (`chef`) to manage the monorepo.

If this was successful, then you're fully set up in sous-chef and can start working on projects.

To check that the `chef` cli is working, run the following command:
```bash
chef --help
```

_It should return a list of commands that you can use._

### 9. Setting up a project specific environment
We use pyenv to manage different Python versions, and Poetry to create virtualenvs with the correct dependencies for each project.

Let's test creating a new Python environment for a project.

<details>
<summary>Windows</summary>

1. cd into a project
```bash
cd cheffelo/sous-chef/projects/data-model
```

2. Create a new virtualenv
```bash
poetry shell
```

3. Install the project dependencies
```bash
poetry install
```

4. Check that you are in a virtualenv
```
which python
```
Should output something like:
```
/Users/<your-username>/cheffelo/sous-chef/projects/data-model/.venv/bin/python
```

</details>

<details>
<summary>macOS</summary>

1. cd into a project
```bash
cd cheffelo/sous-chef/projects/data-model
```

2. Create a new virtualenv
```bash
poetry shell
```

3. Install the project dependencies
```bash
poetry install
```

4. Activate the virtualenv
```bash
source .venv/bin/activate
```

5. Check that you are in a virtualenv
```
which python
```
Should output something like:
```
/Users/<your-username>/cheffelo/sous-chef/projects/data-model/.venv/bin/python
```

</details>

### 10. Connecting to Databricks locally (NOT COMPLETE YET)
If you want to run local code in Databricks, you need to first connect to Databricks using the Databricks extension in your code editor.

1. Click on the Databricks logo in the left-hand side of your code editor.
2. Click on ´Migrate existing project to Databricks´

## Creating a new project or package 🆕
Now we have sous-chef setup, we can start creating new projects and packages using the `chef` cli.

To create a new project, run the new-service target from the command line and provide a name for your service:

```bash
chef create project
```

To create a new package:
```bash
chef create package
```

For more information about the `chef` cli, view [packages/`chef`/README.md](packages/chef/README.md)

### What is the difference between \<library name\>, \<package name\> and \<module name\>?

**Library** name is a human readable name. *E.g: Analytics API*

**Project** name is a name without spaces and upper letters for workflows and folders. *E.g: analytics-api*

**Module** name is the python package name which needs underscores. *E.g: analytics_api*

## Use Cases 💡
In this section, you will find a few use cases to describe how to develop different projects.

### Python Project
In this section will we showcase how to setup a simple ML application that.

#### Create the project
Make sure you have access to the `chef` cli.

> [!NOTE]
> If you do not have access to the `chef` cli. Try running the following:
> ```bash
> poetry shell
> poetry install
> ```

With the chef cli activated, run the following to create a new project:

```bash
chef create project
```

This will prompt you for different questions. However, it is mainly the `Project Name` that needs to be inputted. For the remaining prompts, simply press `Enter`, unless you want to customise it further.

The command will add a basic project structure needed for Python development, and a few files for basic development locally, and on Databricks.

#### Start developing
We are now ready to start adding our custom code.

Open up a new shell / terminal, and move into the project directory. This can be done by running:

```bash
cd projects/<your-project-name>
```

This project will have it's one Python environment, which prohibits conflicting Python packages across projects, but it also enable us to use different Python versions per project.

As a result, we need to create a new virtual environment, and install the project packages again. Therefore, run the following:

```bash
poetry shell
poetry install
```

We can now add external and internal packages with:

```bash
chef add data-contracts  # Internal package at `packages/data-contracts`
chef add streamlit       # External UI package
```

#### Example file

To showcase a simple example. Add the following streamlit app to `app.py`. This will search for recipe embeddings in a vector database.

```python
import asyncio
import streamlit as st

from project_owners.owner import Owner

from data_contracts.recommendations.recipe import RecipeFeatures
from data_contracts.recommendations.store import recommendation_feature_contracts

from aligned import feature_view, String, Bool, FileSource, model_contract
from aligned.exposed_model.ollama import ollama_embedding_contract
from aligned.sources.lancedb import LanceDBConfig

vector_db = LanceDBConfig(path="./vector_db")

recipe = RecipeFeatures()

RecipeEmbedding = ollama_embedding_contract(
    input=recipe.recipe_name,
    entities=recipe.recipe_id,
    model="nomic-embed-text",
    endpoint="http://our-embedding-service:11434",
    contract_name="recipe_embedding",
    contacts=[Owner.matsmoll().markdown()],
    output_source=vector_db.table("recipe_embeddings").as_vector_index("recipes")
)

async def main():
    recipe_to_search = "Laks med soya og ris"
    st.write(f"Searching for '{recipe_to_search}'")

    store = recommendation_feature_contracts()
    store.add_model(RecipeEmbedding)

    with st.spinner("Creating Embeddings"):
        await store.model("recipe_embedding").predict_over(
            RecipeFeatures.query().all()
        ).insert_into_output_source()

    similar_recipes = await (
        store.vector_index("recipes")
            .nearest_n_to({
                "recipe_name": [recipe_to_search],
                number_of_records=5
            }).to_pandas()
    )

    st.title("Similar recipes")
    st.write(similar_recipes)

if __name__ == "__main__":
    asyncio.run(main())
```

#### Run the project
You can run the project locally through Docker with a small application. Ensure that the startup command is added to the `docker-compose.yaml` file first.

```yaml
services:
  app:
    platform: linux/amd64
    build:
      context: ../../
      dockerfile: projects/<project-name>/docker/Dockerfile
    volumes:
      - ./:/opt/projects/<project-name>/
      - ./../../packages:/opt/packages
    command: "python -m streamlit run app.py --server.fileWatcherType poll"

    depends_on:
      - base
    ports:
      - 8500:8501
    env_file:
      - ../../.env
      - .env

    ...
```

Now startup the application with:
```bash
chef up app
```
This will build the project, install everything that is needed and start up the server at `http://127.0.0.1:8500`.

### Data Science

Data science applications are a subtype of a Python project. Meaning you can use everything described in the Python Project use-case.
However, to manage the unpredicability of data and ML could the following also be needed:

- Experiment tracking
- Model versioning - through a model registry
- Feature store - to load offline point-in-time data, and low latency online data.
- Big Brain compute - aka. extra RAM / disk
- Out of memory compute - through Spark / distributed processing
- Job orchestration
- Model serving endpoint
- Monitor and validate data - either data drift or semantic expectations
- Evaluate model online performance
- Explain model outputs

For all of this do we default to the Databricks' components.

Meaning `MLFlow`, `Spark`, Databricks' `feature-engineering` package, `Databricks Asset Bundles`.
However, we still use `Docker` to control the dependencies through the `docker/Dockerfile.databricks` file. See the [databricks-env README](https://github.com/cheffelo/sous-chef/tree/main/packages/databricks-env) for more details.
