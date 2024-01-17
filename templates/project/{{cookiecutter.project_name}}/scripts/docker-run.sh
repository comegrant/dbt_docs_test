#!/usr/bin/env bash

ARGS="$@"

docker run -it --rm \
  --name {{cookiecutter.project_name}} \
  {{cookiecutter.project_name}}:latest \
  $ARGS
