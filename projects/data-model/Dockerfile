FROM databricksruntime/python:14.3-LTS
 
# defaulting to utf-8 to fix a few charset bugs
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
SHELL ["/bin/bash", "-c"]
 
RUN /databricks/python3/bin/pip install 'poetry==1.8.2'
 
# set the home directory
ENV APP_HOME /opt
ENV SERVICE_HOME $APP_HOME/projects/data-model
 
# copy source code
WORKDIR $APP_HOME
COPY packages packages
 
WORKDIR $SERVICE_HOME
COPY projects/data-model/pyproject.toml .
#COPY projects/data-model/poetry.lock .
 
RUN source /databricks/python3/bin/activate \
&& poetry config virtualenvs.create false \
&& poetry install --without=dev

RUN apt-get update
RUN apt-get -y install git

ENV PYTHONPATH /databricks/python3/bin
ENV PATH /databricks/python3/bin:$PATH
 
COPY projects/data-model .