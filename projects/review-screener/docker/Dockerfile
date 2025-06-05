FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME=/opt
ENV SERVICE_HOME=$APP_HOME/projects/review-screener

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/review-screener/pyproject.toml .
COPY projects/review-screener/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p review_screener && touch review_screener/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry

# copy source code
COPY projects/review-screener .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python review_screener/main.py
