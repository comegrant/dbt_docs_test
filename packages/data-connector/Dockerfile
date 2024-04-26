FROM python:3.11-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV PACKAGE_HOME $APP_HOME/packages/data-connector

WORKDIR $APP_HOME
COPY packages packages

WORKDIR $PACKAGE_HOME

# Only copy the package file for imporoved build caching
COPY packages/data-connector/pyproject.toml .
COPY packages/data-connector/poetry.lock .

# create a dummy package to satisfy Poetry and improve caching
RUN mkdir -p data_connector && touch data_connector/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install

# copy source code
COPY packages/data-connector .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

CMD python -m pytest tests
