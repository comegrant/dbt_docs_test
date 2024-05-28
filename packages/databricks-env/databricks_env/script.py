from pathlib import Path


def auto_setup_env() -> None:
    project_dir = Path.cwd().as_posix()
    project_name = project_dir.split("/projects/")[-1].split("/")[0]
    if not project_name:
        project_name = project_dir.split("/.bundle/")[-1].split("/")[0]

    if not project_name:
        raise ValueError("Project name not found")

    setup_env(project_name)


def setup_env(project: str) -> None:
    """
    Setup a project that run on databricks.

    This assumes that the cluster is a Docker based cluster.
    """
    import os
    import sys

    project_dir = f"/opt/projects/{project}"

    current_dir_str = Path.cwd().as_posix()

    if ".bundle" in current_dir_str:
        os.chdir(project_dir)
        return

    if not current_dir_str.startswith("/Workspace"):
        os.chdir(project_dir)
        return

    sys.path.remove(project_dir)
    new_root_dir = (
        current_dir_str.split(f"projects/{project}")[0] + f"projects/{project}"
    )
    os.chdir(new_root_dir)
