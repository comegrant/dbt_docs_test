"""Script to generate the CI a new library."""
from pathlib import Path
from chef.cli import cli

module_name: str = "{{ cookiecutter.module_name }}"
project_name: str = "{{ cookiecutter.project_name }}"

ci_cd_folder = Path(f"../../.github/workflows/{project_name}")
ci_file_path = ci_cd_folder / f"ci.yml"
cd_file_path = ci_cd_folder / f"cd.yml"

ci_cd_folder.mkdir(parents=True, exist_ok=True)

with ci_file_path.open("w") as handle:
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

cd_file_path.write_text(f"""name: Deploy {project_name}

on:
  push:
    branches:
      - 'main'
    paths:
      - '.github/workflows/cd_rec_engine.yml'
      - 'projects/{project_name}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  build-{project_name}-image:
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/{project_name}
      registry: ${{{{ vars.CONTAINER_REGISTRY }}}}
      image-name: {project_name}
      dockerfile: Dockerfile
      tag: ${{{{ github.sha }}}}
    secrets: inherit
""")
