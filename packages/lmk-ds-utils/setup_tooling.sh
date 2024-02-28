#!/usr/bin/env bash

if ! command -v poetry &> /dev/null
then
    if ! command -v pip &> /dev/null
    then
        pip install poetry
    elif ! command -v brew &> /dev/null
    then
	brew install poetry
    else
	echo "Unable to install poetry"
	exit 1
    fi
fi

poetry install

if ! command -v docker &> /dev/null
then
    echo "Docker is not installed. This can be done at https://docs.docker.com/engine/install/"
    exit 1
fi

if ! docker info; then
    echo "Docker is not running. Please start Docker and try again."
    exit 1
fi
