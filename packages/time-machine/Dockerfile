FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir 'poetry==1.8.2'

# set the home directory
ENV APP_HOME /opt
ENV PACKAGE_HOME $APP_HOME/packages/time-machine

WORKDIR $APP_HOME
COPY packages packages

WORKDIR $PACKAGE_HOME

# Only copy the package file for imporoved build caching
COPY packages/time-machine/pyproject.toml .
COPY packages/time-machine/poetry.lock .

# create a dummy package to satisfy Poetry and improve caching
RUN mkdir -p time_machine && touch time_machine/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install

# copy source code
COPY packages/time-machine .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

CMD python -m pytest tests

