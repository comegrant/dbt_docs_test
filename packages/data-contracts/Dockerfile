FROM databricksruntime/python:15.4-LTS

SHELL ["/bin/bash", "-c"]

# Defaulting to utf-8 to fix a few charset bugs
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# set the home directory
ENV APP_HOME=/opt
ENV SERVICE_HOME=$APP_HOME/packages/data-contracts

RUN /databricks/python3/bin/pip install poetry==1.8.2

# copy source code
WORKDIR $APP_HOME
COPY packages packages

WORKDIR $SERVICE_HOME

RUN source /databricks/python3/bin/activate \
	&& echo "$(which poetry)" \
	&& pip install virtualenv==20.31.2 \
	&& poetry config virtualenvs.create false \
	&& poetry install --all-extras && rm -rf ~/.cache/pypoetry/cache


ENV PYTHONPATH=/databricks/python3/bin
ENV PATH=/databricks/python3/bin:$PATH

# store the git branch and commit hash as env variables
ARG GIT_BRANCH
ARG GIT_COMMIT_HASH
ENV GIT_BRANCH=${GIT_BRANCH}
ENV GIT_COMMIT_HASH=${GIT_COMMIT_HASH}
