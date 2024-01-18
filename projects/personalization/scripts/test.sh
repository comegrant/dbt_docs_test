#!/usr/bin/env bash

SERVICE_ROOT=$(realpath $(dirname $0)/../)
ARGS=$@

poetry run pytest
