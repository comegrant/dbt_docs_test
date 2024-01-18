#!/usr/bin/env bash

echo "Creating a new package"

pip install cookiecutter
cookiecutter templates/package -o packages
