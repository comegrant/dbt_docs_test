FROM python:3.10.9-slim-buster

RUN apt-get clean
RUN apt-get update --allow-insecure-repositories --allow-unauthenticated
RUN apt-get install --no-install-recommends -y g++ make libc6-dev unixodbc-dev libpq-dev curl gnupg2

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/cheffelo_personalization

# install Poetry
RUN pip install --no-cache-dir poetry

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/personalization/pyproject.toml .
COPY projects/personalization/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p cheffelo_personalization && touch cheffelo_personalization/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install && rm -rf ~/.cache/pypoetry/cache

# copy source code
COPY projects/personalization .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python cheffelo_personalization/run.py
