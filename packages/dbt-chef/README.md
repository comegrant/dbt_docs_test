# dbt chef

A package that helps working with dbt

## Usage
Run the following command to install this package in your project:

```bash
chef add dbt-chef
```

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.

### Commands

#### Generate Silver Model
Generate a SQL file for a silver layer model.

```bash
dbt-chef generate-silver-model --source-system <source_system> --source-table-name <source_table_name> --model-name <model_name>
```

- `--source-system`: Name of the source system, e.g. cms
- `--source-table-name`: Name of the source table, e.g. cms__company
- `--model-name`: Name of the dbt model to be created, e.g. cms__companies

**Example:**

```bash
dbt-chef generate-silver-model --source-system cms --source-table-name cms__company --model-name cms__companies
```

#### Generate Docs
Generate column documentation for any model.

```bash
dbt-chef generate-docs --model-name <model_name>
```

- `--model-name`: Name of the dbt model

**Example:**

```bash
dbt-chef generate-docs --model-name cms__companies
```

#### Generate YAML
Generate column YAML for any model.

```bash
dbt-chef generate-yaml --model-name <model_name>
```

- `--model-name`: Name of the dbt model

**Example:**

```bash
dbt-chef generate-yaml --model-name cms__companies
```
