"""Script to generate the CI a new library."""

MODULE_NAME: str = "{{ cookiecutter.module_name }}"
PACKAGE_NAME: str = "{{ cookiecutter.package_name }}"

CI_FILE_PATH: str = f"../../.github/workflows/ci_{MODULE_NAME}.yml"

with open(CI_FILE_PATH, "w") as handle:
    handle.writelines(
        f"""---
name: CI packages/{{ cookiecutter.package_name }}

on:
  pull_request:
    paths:
      - '.github/workflows/python_reusable.yml'
      - '.github/workflows/ci_{MODULE_NAME}.yml'
      - 'packages/{MODULE_NAME}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  ci-libs-{PACKAGE_NAME}:
    uses:
      ./.github/workflows/python_reusable.yml
    with:
      working-directory: packages/{MODULE_NAME}
    secrets: inherit"""
    )
