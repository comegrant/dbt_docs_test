FROM python:3.11-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV PACKAGE_HOME $APP_HOME/packages/model-registry

WORKDIR $APP_HOME
COPY packages packages

WORKDIR $PACKAGE_HOME

# Only copy the package file for improved build caching
COPY packages/model-registry/pyproject.toml .
COPY packages/model-registry/poetry.lock .

# create a dummy package to satisfy Poetry and improve caching
RUN mkdir -p model_registry && touch model_registry/__init__.py

# install requirements
RUN pip install virtualenv==20.34.0
RUN poetry config virtualenvs.create false
RUN poetry install --all-extras

# copy source code
COPY packages/model-registry .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

CMD python -m pytest tests
