#!/usr/bin/env bash

ARGS="$@"

docker run -it --rm \
  --name personalization \
  personalization:latest \
  $ARGS
