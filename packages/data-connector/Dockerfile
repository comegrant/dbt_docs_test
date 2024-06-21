FROM databricksruntime/python:14.3-LTS

SHELL ["/bin/bash", "-c"]

# Defaulting to utf-8 to fixe a few charset bugs
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/packages/data-connector/

RUN /databricks/python3/bin/pip install poetry

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME
COPY packages/data-connector/pyproject.toml .
COPY packages/data-connector/poetry.lock .

# create a dummy package to satisfy Poetry
RUN mkdir -p data_connector && touch data_connector/__init__.py

RUN source /databricks/python3/bin/activate \
	&& echo "$(which poetry)" \
	&& poetry config virtualenvs.create false \
	&& poetry install --with=dev


ENV PYTHONPATH /databricks/python3/bin
ENV PATH /databricks/python3/bin:$PATH

# copy source code
COPY packages/data-connector .

WORKDIR $SERVICE_HOME