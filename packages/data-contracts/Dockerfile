FROM python:3.11-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry=='1.8.2'

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/packages/data-contracts

# copy source code
WORKDIR $APP_HOME
COPY packages packages

# Global linting config purposes
COPY pyproject.toml .

WORKDIR $SERVICE_HOME
# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --with=dev -n && rm -rf ~/.cache/pypoetry/cache
