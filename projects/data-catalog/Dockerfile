FROM matsmoll/aligned-catalog-free:experimental-latest

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /app
ENV SERVICE_HOME $APP_HOME/projects/data-catalog

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
COPY projects/data-catalog/pyproject.toml .
COPY projects/data-catalog/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p data_catalog && touch data_catalog/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --only main -n && rm -rf ~/.cache/pypoetry/cache
RUN pip install polars-lts-cpu==0.20.31

# copy source code
COPY projects/data-catalog .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
