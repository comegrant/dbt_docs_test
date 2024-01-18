#!/usr/bin/env bash

ARGS="$@"
poetry run python churn_ai/run.py $ARGS
