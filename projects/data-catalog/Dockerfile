FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/data-catalog

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/data-catalog/pyproject.toml .
COPY projects/data-catalog/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p data_catalog && touch data_catalog/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --only main -n && rm -rf ~/.cache/pypoetry/cache

# copy source code
COPY projects/data-catalog .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python -m streamlit run data_catalog/main.py
