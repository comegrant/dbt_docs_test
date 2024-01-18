#!/usr/bin/env bash

echo "Creating a new service"

pip install cookiecutter
cookiecutter templates/project -o projects
