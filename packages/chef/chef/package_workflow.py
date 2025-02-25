from pathlib import Path


def test_package_workflow(package_name: str, file_name: str, python_version: str) -> str:
    return f"""name: {package_name} - Package PR
on:
  pull_request:
    paths:
      - '{file_name}'
      - 'packages/{package_name}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  test-{package_name}:
    uses:
      ./.github/workflows/template_test_python.yml
    with:
      python-version: "{python_version}"
      working-directory: packages/{package_name}
    secrets: inherit"""


def write_package_workflow(package_file: Path, root_dir: Path, package_name: str, python_version: str) -> None:
    package_file.write_text(
        test_package_workflow(
            package_name,
            package_file.resolve().relative_to(root_dir.resolve()).as_posix(),
            python_version=python_version,
        )
    )
