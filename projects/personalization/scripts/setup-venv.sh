#!/usr/bin/env bash

PYTHON_VERSION=$(cat pyproject.toml | grep "python = " | sed 's/[^0-9.]*//g')
echo "Using python version $PYTHON_VERSION"
NAME=$(basename $(pwd))
echo "Creating virtualenv $NAME"

pyenv virtualenv $PYTHON_VERSION $NAME
