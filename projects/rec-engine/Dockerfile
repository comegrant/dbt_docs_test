FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/rec-engine

# Installing utils deps
RUN apt-get update && \
    apt-get install --no-install-recommends -y g++ make libc6-dev unixodbc-dev libpq-dev curl gnupg2

# Install Microsoft ODBC driver for SQL Server
RUN apt-get install -y apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install --no-install-recommends -y msodbcsql18 mssql-tools18

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/rec-engine/pyproject.toml .
COPY projects/rec-engine/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p rec_engine && touch rec_engine/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry/cache
RUN pip install polars-lts-cpu

# copy source code
COPY projects/rec-engine .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
CMD python rec_engine/main.py
