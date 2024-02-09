FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV PACKAGE_HOME $APP_HOME/packages/pydantic-argparser

WORKDIR $APP_HOME
COPY packages packages

WORKDIR $PACKAGE_HOME

# Only copy the package file for imporoved build caching
COPY packages/pydantic-argparser/pyproject.toml .
COPY packages/pydantic-argparser/poetry.lock .

# create a dummy package to satisfy Poetry and improve caching
RUN mkdir -p pydantic_argparser && touch pydantic_argparser/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install

# copy source code
COPY packages/pydantic-argparser .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

CMD python -m pytest tests
