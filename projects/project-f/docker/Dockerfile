FROM python:3.11-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME=/opt
ENV SERVICE_HOME=$APP_HOME/projects/project-f

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/project-f/pyproject.toml .
COPY projects/project-f/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p project_f && touch project_f/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry

# copy source code
COPY projects/project-f .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python project_f/main.py
