#!/usr/bin/env bash

ARGS="$@"

docker run -it --rm \
  --name churn-ai \
  churn-ai:latest \
  $ARGS
