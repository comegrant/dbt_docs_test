"""Utility functions for the chef CLI."""

import subprocess
from pathlib import Path

import click


def echo_action(action: str) -> None:
    """Echo an action with consistent formatting."""
    click.echo(click.style("Yes Chef!", bold=True))
    click.echo(action)


def root_dir() -> Path:
    """Find the mono_repo directory containing the .git folder."""
    current_dir = Path()
    while not (current_dir / ".git").is_dir():
        if current_dir == Path("/"):
            raise ValueError("Could not find sous_chef directory containing git")
        if current_dir.as_posix().startswith("."):
            current_dir = current_dir / ".."
        else:
            current_dir = current_dir.parent

    # Return the packages directory within mono_repo
    return current_dir


def workflow_dir() -> Path:
    """Get the workflow directory path."""
    return root_dir() / ".github" / "workflows"


def internal_package_path() -> Path:
    """Get the internal packages path."""
    return root_dir() / "packages"


def projects_path() -> Path:
    """Get the projects path."""
    return root_dir() / "projects"


def template_path() -> Path:
    """Get the template path."""
    return root_dir() / "templates"


def list_dirs_in(path: Path) -> list[str]:
    """List directory names in a given path."""
    return [package.name for package in path.iterdir() if package.is_dir()]


def path_for_internal_package(name: str) -> Path:
    """Get the path for an internal package."""
    return internal_package_path() / name


def is_root() -> bool:
    """Check if current directory is the root."""
    return Path(".git").is_dir()


def is_project() -> bool:
    """Check if current directory is a project."""
    return Path().resolve().parent.name == "projects"


def folder_name() -> str | Exception:
    """Get the current folder name."""
    return Path().resolve().name


def internal_packages() -> list[str]:
    """Get list of internal packages."""
    return list_dirs_in(internal_package_path())


def internal_projects() -> list[str]:
    """Get list of internal projects."""
    return list_dirs_in(projects_path())


def is_existing_package(name: str) -> bool:
    """Check if a package exists."""
    import importlib

    try:
        importlib.import_module(name)
        return True
    except ImportError:
        return False


def compose_path(name: str) -> Path:
    """Get the compose path for a project or package."""
    if name in internal_projects():
        return projects_path() / name
    return internal_package_path() / name


def set_git_config(key: str, value: str) -> None:
    """Set a git configuration value."""
    subprocess.run(["git", "config", "--global", key, value], check=False)


def read_command(command: list[str]) -> str | None:
    """Read the output of a command."""
    try:
        return subprocess.check_output(command).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        return None


def git_config(key: str) -> str | None:
    """Get a git configuration value."""
    return read_command(["git", "config", key])


def default_create_context() -> dict[str, str]:
    """Get default context for project creation."""
    return {}


def should_prompt_user() -> bool:
    """Check if user should be prompted."""
    return True


def is_module(module_name: str) -> bool:
    """Check if a string represents a module."""
    path = module_name.replace(".", "/").split(":")[0] + ".py"
    return Path(path).is_file()
