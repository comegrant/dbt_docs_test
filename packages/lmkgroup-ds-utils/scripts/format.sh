#!/usr/bin/env bash

PACKAGE_ROOT=$(realpath $(dirname $0)/../)
SRC_FOLDER=$PACKAGE_ROOT/lmkgroup_ds_utils
TESTS_FOLDER=$PACKAGE_ROOT/tests
ARGS=$@

poetry run black $SRC_FOLDER $TESTS_FOLDER $ARGS
