"""Script to generate the CI a new library."""
from pathlib import Path

MODULE_NAME: str = "{{ cookiecutter.module_name }}"
PACKAGE_NAME: str = "{{ cookiecutter.package_name }}"

CI_FILE_PATH: str = f"../../.github/workflows/ci_{MODULE_NAME}.yml"

with Path(CI_FILE_PATH).open("w") as handle:
    handle.writelines(
        f"""---
name: CI packages/{{ cookiecutter.package_name }}

on:
  pull_request:
    paths:
      - '.github/workflows/python_reusable.yml'
      - '.github/workflows/ci_{MODULE_NAME}.yml'
      - 'packages/{PACKAGE_NAME}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  ci-libs-{PACKAGE_NAME}:
    uses:
      ./.github/workflows/python_reusable.yml
    with:
      working-directory: packages/{PACKAGE_NAME}
    secrets: inherit""",
    )
