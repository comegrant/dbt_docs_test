FROM python:{{cookiecutter.python_version}}-slim-buster

# install Poetry
RUN pip install --no-cache-dir poetry

# set the home directory
ENV APP_HOME /opt
ENV PACKAGE_HOME $APP_HOME/packages/{{cookiecutter.package_name}}

WORKDIR $APP_HOME
COPY packages packages

WORKDIR $PACKAGE_HOME

# Only copy the package file for imporoved build caching
COPY packages/{{cookiecutter.package_name}}/pyproject.toml .
COPY packages/{{cookiecutter.package_name}}/poetry.lock .

# create a dummy package to satisfy Poetry and improve caching
RUN mkdir -p {{cookiecutter.module_name}} && touch {{cookiecutter.module_name}}/__init__.py

# install requirements
RUN poetry config virtualenvs.create false
RUN poetry install

# copy source code
COPY packages/{{cookiecutter.package_name}} .

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}

CMD python -m pytest tests
