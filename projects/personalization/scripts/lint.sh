#!/usr/bin/env bash

SERVICE_ROOT=$(realpath $(dirname $0)/../)
SRC_FOLDER=$SERVICE_ROOT/personalization
TESTS_FOLDER=$SERVICE_ROOT/tests
ARGS=$@

poetry run ruff $SRC_FOLDER $TESTS_FOLDER $ARGS
