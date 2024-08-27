FROM python:3.10-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/order-forecasting

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/orders-forecasting/pyproject.toml .
COPY projects/orders-forecasting/poetry.lock .
COPY projects/orders-forecasting/configs configs
COPY projects/orders-forecasting/sqls sqls
COPY projects/orders-forecasting/tests tests
COPY projects/orders-forecasting/orders_forecasting orders_forecasting

# create a dummy package to satisfy Poetry
RUN mkdir -p orders_forecasting && touch orders_forecasting/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry/cache

# copy source code
COPY projects/orders-forecasting .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

# set starting command
WORKDIR $SERVICE_HOME
