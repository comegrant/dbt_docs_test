"""Script to generate the CI a new library."""

MODULE_NAME: str = "{{ cookiecutter.module_name }}"
PROJECT_NAME: str = "{{ cookiecutter.project_name }}"

CI_FILE_PATH: str = f"../../.github/workflows/ci_{MODULE_NAME}.yml"

with open(CI_FILE_PATH, "w") as handle:
    handle.writelines(
        f"""name: CI {PROJECT_NAME}
on:
  pull_request:
    paths:
      - '.github/workflows/python_reusable.yml'
      - '.github/workflows/ci_{MODULE_NAME}.yml'
      - 'projects/{PROJECT_NAME}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  test-{PROJECT_NAME}:
    uses:
      ./.github/workflows/test_in_docker.yml
    with:
      base-service: base
      test-service: test
      working-directory: projects/{PROJECT_NAME}
    secrets: inherit

  build-{PROJECT_NAME}-image:
    needs: test-{PROJECT_NAME}
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/{PROJECT_NAME}
      registry: ${{ '{{' }} vars.CONTAINER_REGISTRY {{ '}}' }}
      image-name: {PROJECT_NAME}
      dockerfile: docker/Dockerfile.databricks
      tag: dev-latest
    secrets: inherit"""
    )
