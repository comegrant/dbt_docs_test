<h1 align="center">
Chef
</h1>

<p align="center">
<img src="../../assets/chef/michelle-lind.png" alt="michelle-lind" width="200"/>
</p>

<p align="center">
Chef is a cli tool to simplify the development of Cheffelo projects and packages.
</p>

## TLDR

```bash
poerty shell
poetry install
chef --help
```


## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Commands](#commands)
  - [dbt Commands](#dbt-commands)
  - [PowerBI Commands](#powerbi-commands)

## Installation

The CLI should be part of each project's environment, so all you need to do is to activate the project's environment. By running:

```bash
poetry shell
poetry install
```


## Usage

If you have activated the poetry environment in your terminal, run:

```bash
chef <command>
```

## Commands

To find a list of commands you can use, run:

```bash
chef --help
```

Below is the full list of different commands available.

### Create
Create a new project or package.

```bash
chef create <project | package>
```

This will start up an interactable wizard that fills in basic information.

### Add
Adds an internal or external package to the project.

```bash
chef add project-owners
```

If you define a package that matches one of the internal packages, the internal package will be added.
Otherwise it will assume it is an external package, and add that one.

### Remove
Removes a dependency from the project.

```bash
chef remove project-owners
```

### List packages
Lists the internal packages that exists.

```bash
chef list-packages
```

### Pin
Pins an internal dependency to a specified version.
If no versions are defined, it will select the current version on origin/main.

```bash
chef pin project-owners
```

### Expose
Exposes a port to a public url.
Can be used to showcase experimental projects to some other data chefs.

```bash
chef expose <port>
```

### Build
Builds a project into a Docker image. Will default to the name of the project.

```bash
chef build
```


### Up
Spins up a complete environment of the project.
This is supposed to simulate the project in it's final form.

```bash
chef up
```

You can also specify a `--profile` if you would like to spin up a subset of services.

### Down
To take down all services after running `chef up` can you use `chef down`

### Run
Runs a command in the projects environment.

```bash
chef run echo "Hello World"
```

### Shell
Opens up an interactable bash shell in a Docker image.

```bash
chef shell
```

This can be very useful for debugging Docker images, or specific commands.

### Test
Runs all tests in the current package or project.

This assumes that all tests are located at `./tests`.

```bash
chef test
```

### Generate Dotenv
Generates a `.env` file that contains the expected settings for the project.

```bash
chef generate-dotenv
```

If you do not provide a `--settings-path`, then it will default to look for a `settings.py` file and a `pydantic_settings.BaseSettings` class called `Settings`.

### Lint
Run a linter in the project.

```bash
chef lint
```

### Format
Runs a formatter on the codebase

```bash
chef format
```

### Doctor
Checks the project for common issues.

```bash
chef doctor
```

You can also pass a `--project` argument to check a specific project. For example `chef doctor --project data-model`

Note: when testing the `data-model` project, you can't be in a directory lower than the root `data-model` directory.

### dbt Commands

The chef CLI includes integrated dbt development tools. These commands must be run from the `projects/data-model/transform` directory.

#### Generate Silver Model
Generate a SQL file for a silver layer model.

```bash
chef dbt generate-silver-model --source-system <source_system> --source-table-name <source_table_name> --model-name <model_name>
```

- `--source-system`: Name of the source system, e.g. cms
- `--source-table-name`: Name of the source table, e.g. cms__company
- `--model-name`: Name of the dbt model to be created, e.g. cms__companies

**Example:**
```bash
chef dbt generate-silver-model --source-system cms --source-table-name cms__company --model-name cms__companies
```

#### Generate Docs
Generate column documentation for any model.

```bash
chef dbt generate-docs --model-name <model_name>
```

- `--model-name`: Name of the dbt model

**Example:**
```bash
chef dbt generate-docs --model-name cms__companies
```

#### Generate YAML
Generate column YAML for any model.

```bash
chef dbt generate-yaml --model-name <model_name>
```

- `--model-name`: Name of the dbt model

**Example:**
```bash
chef dbt generate-yaml --model-name cms__companies
```

#### Generate PowerBI TMDL
Generate PowerBI TMDL file for any model.

```bash
chef dbt generate-tmdl --model-name <model_name>
```

- `--model-name`: Name of the dbt model

**Example:**
```bash
chef dbt generate-tmdl --model-name cms__companies
```

#### Prepare Local Development
Prepare local development environment by dropping tables in personal schemas and updating states.

```bash
chef dbt prepare-local-development
```

#### Update States
Update dbt states for different environments (dev, test, prod).

```bash
chef dbt update-states
```

### PowerBI Commands

The chef CLI includes integrated PowerBI development tools. These commands must be run from the `projects/powerbi` directory.

#### Fix Opening Page
Update activePageName in all pages.json files to the first page in pageOrder. This ensures consistency across PowerBI reports by setting the active page to always be the first page in the page order.

```bash
chef powerbi fix-opening-page
```

This command:
- Searches for all `pages.json` files in your PowerBI directory
- Updates the `activePageName` in each file to match the first page in the `pageOrder` array
- Displays a summary of changes made
