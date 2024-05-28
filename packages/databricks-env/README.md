# Databricks Env

This package makes it easy to swap between dev use-cases and production use-cases based on which environment we run in.

This is due to Databricks uploading the "new" files to a `Workspace` dir.
Therefore, this package checks which path we start in, and switches to the most appropeate one.

## Usage
Run the following command to install this package:

```bash
chef add databricks-env
```
Then add the following as the first cell in the databricks notebook.


```python
from databricks_env import auto_setup_env
auto_setup_env()
```

or if it is not able to find the project, define it manually with.

```python
from databricks_env import setup_env

setup_env("my_project")
```

## Develop

### Installation
To develop on this package, install all the dependencies with `poetry install`.

Then make your changes with your favorite editor of choice.

## Prerequisit

To develop applications on Databricks, it assumes the following.

1. You have a cluster that is based on a docker image for your related project.
2. You either run the notebook through Databricks Asset Bundles, or through the Databricks extension.

### Build docker image

Add the following as `Dockerfile.databricks` and fill in `{{project-name}}` and `{{module-name}}`.

> **Warning**
>
> Make sure that the base image matches the scala version of the cluster.
> E.g. `FROM databricksruntime/python:14.3-LTS` means that the cluster should be set to `14.3-LTS`.


```Dockerfile
FROM databricksruntime/python:14.3-LTS

SHELL ["/bin/bash", "-c"]

RUN /databricks/python3/bin/pip install poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/{{project-name}}

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/{{project-name}}/pyproject.toml .
COPY projects/{{project-name}}/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p {{module-name}} && touch {{module-name}}/__init__.py

RUN source /databricks/python3/bin/activate \
	&& echo "$(which poetry)" \
	&& poetry config virtualenvs.create false \
	&& poetry install --without=dev


ENV PYTHONPATH /databricks/python3/bin
ENV PATH /databricks/python3/bin:$PATH

COPY projects/{{project-name}}/{{module-name}} {{module-name}}
```

### Add `databricks-env` as a dependency

Run `chef add databricks-env` in the project directory, as this is needed in you databricks notebook.

### Add notebook file

Add a notebook file that contains the pipeline that you would like.
However, remember to start the file with the following

```python
# Databricks notebook source
# COMMAND ----------
from databricks_env import auto_setup_env
auto_setup_env()

# COMMAND ----------
Your code here...
```

### Databricks Asset Bundles

Add the databricks asset bundle as `databricks.yml` to your project directory.

Fill in the `{{project-name}}` and the `{{notebook-path}}`.

```yaml
bundle:
  name: {{project-name}}

variables:
  docker_image_url:
    description: The url of the source code. in the form as a Docker image.

workspace:
  host: https://adb-4291784437205825.5.azuredatabricks.net

resources:
  jobs:
    {{project-name}}-notebook:
      name: ${bundle.target}-{{project-name}}-notebook

      job_clusters:
        - job_cluster_key: {{project-name}}-cluster
          new_cluster:
            num_workers: 1
            spark_version: 14.3.x-scala2.12
            node_type_id: Standard_DS4_v2
            docker_image:
              url: "${var.docker_image_url}"
              basic_auth:
                username: "{{secrets/auth_common/docker-registry-username}}"
                password: "{{secrets/auth_common/docker-registry-password}}"

      # schedule:
      #   quartz_cron_expression: '0 0 0 ? * MON'
      #   timezone_id: UTC

      # email_notifications:
      #   on_failure:
      #     - mats.mollestad@cheffelo.com
      #   on_success:
      #     - mats.mollestad@cheffelo.com

      tasks:
        - task_key: NotebookTaskKey
          job_cluster_key: {{project-name}}-cluster
          notebook_task:
            notebook_path: {{notebook-path}}

targets:
  # The 'dev' target, used for development purposes.
  # Whenever a developer deploys using 'dev', they get their own copy.
  dev:
    # We use 'mode: development' to make sure everything deployed to this target gets a prefix
    # like '[dev my_user_name]'. Setting this mode also disables any schedules and
    # automatic triggers for jobs and enables the 'development' mode for Delta Live Tables pipelines.
    mode: development
    default: true

  test:
    workspace:
      host: https://adb-3194696976104051.11.azuredatabricks.net
```


### CI pipelines

Add a file in `.github/workflows/{{file_name}}.yml` with something like the following.
Again fill in the `{{project-name}}` in the template bellow.

```yaml
name: CI {{project-name}}

on:
  pull_request:
    paths:
      - 'projects/{{project-name}}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  build-image:
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/{{project-name}}
      registry: ${{ vars.CONTAINER_REGISTRY }}
      image-name: {{project-name}}-databricks
      dockerfile: Dockerfile.databricks
      tag: dev-latest
    secrets: inherit

  deploy-to-dev:
    needs: build-image
    uses: ./.github/workflows/cd_dab_template.yml
    with:
      working-directory: projects/{{project-name}}
      target-env: Development

      # Setting the docker image which is needed for python projects
      extra-args: '--var="docker_image_url=${{ vars.CONTAINER_REGISTRY }}/{{project-name}}-databricks:dev-latest"'
    secrets: inherit
```
