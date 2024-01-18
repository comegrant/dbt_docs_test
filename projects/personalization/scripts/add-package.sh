#!/usr/bin/env bash

PACKAGE_NAME=$1

if [ -z "$PACKAGE_NAME" ]; then
  echo "Please provide a package name"
  exit 1
fi

poetry add --editable "../../packages/$1"
