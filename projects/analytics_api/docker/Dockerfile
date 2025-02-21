FROM python:3.11-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/analytics_api

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/analytics_api/pyproject.toml .
COPY projects/analytics_api/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p analytics_api && touch analytics_api/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry/cache

# copy source code
COPY projects/analytics_api .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python analytics_api/main.py
