FROM python:{{cookiecutter.python_version}}-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME=/opt
ENV SERVICE_HOME=$APP_HOME/projects/{{cookiecutter.project_name}}

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/{{cookiecutter.project_name}}/pyproject.toml .
COPY projects/{{cookiecutter.project_name}}/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p {{cookiecutter.module_name}} && touch {{cookiecutter.module_name}}/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry

# copy source code
COPY projects/{{cookiecutter.project_name}} .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python {{cookiecutter.module_name}}/main.py
