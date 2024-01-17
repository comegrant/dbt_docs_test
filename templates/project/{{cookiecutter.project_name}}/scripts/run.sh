#!/usr/bin/env bash

ARGS="$@"
poetry run python {{cookiecutter.module_name}}/run.py $ARGS
