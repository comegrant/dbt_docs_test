#!/usr/bin/env bash

NAME=$(basename $(pwd))
REPO_ROOT=$(realpath $(dirname $0)/../../../)
PROJECT_ROOT=${REPO_ROOT}/projects/${NAME}
DOCKERFILE_PATH=${PROJECT_ROOT}/Dockerfile
ARGS=("$@")

echo "Building docker image for ${SERVICE_ROOT} with args ${ARGS[@]}"

docker build -t test:latest -f ${DOCKERFILE_PATH} ${REPO_ROOT} ${ARGS[@]}
