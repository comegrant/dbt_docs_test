FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/ml-feature-store

# copy source code
WORKDIR $APP_HOME
COPY packages packages

# Global linting config purposes
COPY pyproject.toml .

WORKDIR $SERVICE_HOME
COPY projects/ml-feature-store/pyproject.toml .
COPY projects/ml-feature-store/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p ml_feature_store && touch ml_feature_store/__init__.py

# install requirements
ARG DEP_GROUP
RUN poetry config virtualenvs.create false
RUN poetry install ${DEP_GROUP} -n && rm -rf ~/.cache/pypoetry/cache

# copy source code
COPY projects/ml-feature-store .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME