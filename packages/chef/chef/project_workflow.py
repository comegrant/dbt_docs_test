def test_project_workflow(project_name: str, file_name: str, python_version: str) -> str:
    return f"""name: {project_name} - Project PR
on:
  pull_request:
    paths:
      - '{file_name}'
      - 'projects/{project_name}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  test-{project_name}:
    uses:
      ./.github/workflows/template_test_python.yml
    with:
      python-version: "{python_version}"
      working-directory: projects/{project_name}
    secrets: inherit

  build-{project_name}-databricks-image:
    needs: test-{project_name}
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/{project_name}
      registry: ${{{{ vars.CONTAINER_REGISTRY }}}}
      image-name: {project_name}
      dockerfile: docker/Dockerfile.databricks
      tag: dev-latest
    secrets: inherit

  deploy-{project_name}-to-dev:
    uses: ./.github/workflows/cd_dab_template.yml
    with:
      working-directory: projects/{project_name}
      target-env: Development
      docker-image-name: {project_name}
      docker-image-tag: dev-latest
    secrets: inherit"""


def deploy_project_workflow(project_name: str, file_name: str) -> str:
    return f"""name: {project_name} deploy
on:
  push:
    branches:
      - 'main'
    paths:
      - '{file_name}'
      - 'projects/{project_name}/**'
  workflow_dispatch:  # Allows to trigger the workflow manually in GitHub UI

jobs:
  build-{project_name}-image:
    uses: ./.github/workflows/cd_docker_image_template.yml
    with:
      working-directory: projects/{project_name}
      registry: ${{{{ vars.CONTAINER_REGISTRY }}}}
      image-name: {project_name}
      dockerfile: docker/Dockerfile.databricks
      tag: ${{{{ github.sha }}}}
    secrets: inherit"""
