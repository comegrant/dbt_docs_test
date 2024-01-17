#!/usr/bin/env bash

SERVICE_ROOT=$(realpath $(dirname $0)/../)
SRC_FOLDER=$SERVICE_ROOT/{{cookiecutter.module_name}}
TESTS_FOLDER=$SERVICE_ROOT/tests
ARGS=$@

poetry run ruff $SRC_FOLDER $TESTS_FOLDER $ARGS
