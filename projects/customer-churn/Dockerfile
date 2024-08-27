FROM python:3.10-slim-buster

RUN apt-get update -y && apt-get install -y gcc && apt-get install -y build-essential git libgomp1 libpq-dev python3-dev

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/customer-churn

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY projects/customer-churn/pyproject.toml .
COPY projects/customer-churn/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p customer_churn && touch customer_churn/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install --without=dev -n && rm -rf ~/.cache/pypoetry/cache

COPY projects/customer-churn/configs configs
COPY projects/customer-churn/tests tests
COPY projects/customer-churn/customer_churn customer_churn
