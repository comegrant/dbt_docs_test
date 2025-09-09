"""Doctor command check functions for the chef CLI."""

import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from chef.doctor_config import (
    ESSENTIAL_PROJECT_FILES,
    GIT_BRANCH_PATTERN,
    MAX_DAYS_SINCE_REBOOT,
    MINIMUM_REQUIRED_POETRY_VERSION,
    MINIMUM_REQUIRED_PYENV_VERSION_MAC,
    MINIMUM_REQUIRED_PYENV_VERSION_WINDOWS,
    MINIMUM_REQUIRED_PYTHON_VERSION,
    VSCODE_DATABRICKS_EXTENSION_URL,
)
from chef.utils import git_config, projects_path

logger = logging.getLogger(__name__)


def check_python_version() -> bool:
    """Check if Python version meets requirements."""
    current_version = sys.version_info[:2]

    if current_version >= MINIMUM_REQUIRED_PYTHON_VERSION:
        click.echo(
            click.style(
                f"✅ Python {'.'.join(map(str, current_version))} is installed "
                f"(>={'.'.join(map(str, MINIMUM_REQUIRED_PYTHON_VERSION))})",
                fg="green",
            )
        )
        return True
    else:
        click.echo(
            click.style(
                f"❌ Python {'.'.join(map(str, current_version))} is installed, "
                f"but {'.'.join(map(str, MINIMUM_REQUIRED_PYTHON_VERSION))} or higher is required",
                fg="red",
            )
        )
        return False


def check_poetry_installation() -> bool:
    """Check if Poetry is installed and meets version requirements."""
    if not shutil.which("poetry"):
        click.echo(click.style("❌ Poetry is not installed", fg="red"))
        return False

    try:
        result = subprocess.run(["poetry", "--version"], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError:
        click.echo(click.style("❌ Failed to get Poetry version", fg="red"))
        return False

    version_match = re.search(r"(\d+\.\d+\.\d+)", result.stdout)
    if not version_match:
        click.echo(click.style("❌ Unable to parse Poetry version", fg="red"))
        return False

    version_str = version_match.group(1)
    version = tuple(map(int, version_str.split(".")))

    if version < MINIMUM_REQUIRED_POETRY_VERSION:
        click.echo(
            click.style(
                f"❌ Poetry {version_str} is installed, "
                f"but version {'.'.join(map(str, MINIMUM_REQUIRED_POETRY_VERSION))} or higher is required",
                fg="red",
            )
        )
        return False

    click.echo(
        click.style(
            f"✅ Poetry {version_str} is installed (>={'.'.join(map(str, MINIMUM_REQUIRED_POETRY_VERSION))})",
            fg="green",
        )
    )
    return True


def check_docker_installation() -> bool:
    """Check if Docker is installed."""
    if shutil.which("docker"):
        click.echo(click.style("✅ Docker is installed", fg="green"))
        return True
    else:
        click.echo(click.style("❌ Docker is not installed", fg="red"))
        return False


def check_git_configuration() -> bool:
    """Check if Git is properly configured."""
    email = git_config("user.email")
    name = git_config("user.name")

    if email and name:
        click.echo(click.style("✅ Git is configured with user name and email", fg="green"))
        return True

    click.echo(click.style("❌ Git is not fully configured", fg="red"))
    if not email:
        click.echo(
            "  - Missing user.email in git config, add it by running: git config --global user.email 'you@example.com'"
        )
    if not name:
        click.echo("  - Missing user.name in git config, add it by running: git config --global user.name 'Your Name'")
    return False


def check_project(project_name: str) -> bool:
    """Check if a project exists and has essential files."""
    project_path = projects_path() / project_name

    if not project_path.exists():
        click.echo(click.style(f"❌ Project '{project_name}' does not exist", fg="red"))
        return False
    else:
        click.echo(click.style(f"✅ Project '{project_name}' exists in the project directory", fg="green"))

    all_passed = True

    # Check for essential project files
    for file in ESSENTIAL_PROJECT_FILES:
        if (project_path / file).exists():
            click.echo(click.style(f"✅ {file} exists in the project", fg="green"))
        else:
            click.echo(click.style(f"❌ {file} is missing from the project", fg="red"))
            all_passed = False

    return all_passed


def check_last_rebase() -> bool:
    """Check if branch is up to date with origin/main."""
    try:
        # Fetch the latest changes from origin
        subprocess.run(["git", "fetch", "origin", "main"], check=True, capture_output=True)

        # Get the number of commits that origin/main is ahead of the current branch
        result = subprocess.run(
            ["git", "rev-list", "--count", "HEAD..origin/main"], check=True, capture_output=True, text=True
        )
        commits_behind = int(result.stdout.strip())

        if commits_behind == 0:
            click.echo(click.style("✅ Branch is up to date with origin/main", fg="green"))
            return True
        else:
            click.echo(
                click.style(
                    f"❓ Branch is behind origin/main by {commits_behind} commit(s), please consider rebasing",
                    fg="yellow",
                )
            )
            return False
    except subprocess.CalledProcessError:
        click.echo(click.style("❌ Unable to check if branch is up to date. Are you in a git repository?", fg="red"))
        return False


def check_git_branch_format() -> bool:
    """Check if git branch follows the correct format."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], check=True, capture_output=True, text=True
        )
        current_branch = result.stdout.strip()

        if re.match(GIT_BRANCH_PATTERN, current_branch):
            click.echo(click.style(f"✅ Git branch '{current_branch}' follows the correct format", fg="green"))
            return True
        else:
            click.echo(
                click.style(f"❌ Git branch '{current_branch}' does not follow the format name/feature-name", fg="red")
            )
            return False
    except subprocess.CalledProcessError:
        click.echo(click.style("❌ Unable to check git branch format. Are you in a git repository?", fg="red"))
        return False


def check_user_in_owners() -> bool:
    """Check if user is in the owners package."""
    from project_owners.owner import owner_for_email

    email = git_config("user.email")
    if not email:
        click.echo(
            click.style("❌ Unable to check if user is in owners package. Git email is not configured.", fg="red")
        )
        return False

    owner = owner_for_email(email)
    if owner:
        click.echo(click.style(f"✅ User with email '{email}' is in the owners package", fg="green"))
        return True
    else:
        click.echo(click.style(f"❌ User with email '{email}' is not in the owners package", fg="red"))
        click.echo("  Please add your info to the project-owners.owner:Owner.all_owners() function.")
        return False


def check_vscode_databricks_extension() -> bool:
    """Check if VSCode Databricks extension is installed."""
    if platform.system() == "Windows":
        vscode_path = Path(os.environ.get("APPDATA", "")) / "Code" / "User" / "extensions"
    else:
        vscode_path = Path.home() / ".vscode" / "extensions"

    if not vscode_path.exists():
        click.echo(click.style("❓ VSCode not detected or not using the default extensions path", fg="yellow"))
        return True  # Not a failure, just not applicable

    databricks_extension = next((ext for ext in vscode_path.glob("*databricks*") if ext.is_dir()), None)

    if not databricks_extension:
        click.echo(click.style("❌ Databricks extension is not installed in VSCode", fg="red"))
        click.echo(f"  You can install it from the VSCode marketplace: {VSCODE_DATABRICKS_EXTENSION_URL}")
        return False

    package_json = databricks_extension / "package.json"
    if not package_json.exists():
        click.echo(click.style("✅ Databricks extension is installed (version unknown)", fg="green"))
        return True

    with package_json.open() as f:
        data = json.load(f)
        version = data.get("version", "Unknown")
        click.echo(click.style(f"✅ Databricks extension {version} is installed", fg="green"))
    return True


def check_last_reboot() -> bool:
    """Check when the system was last rebooted."""
    try:
        if platform.system() == "Darwin":  # macOS
            result = subprocess.run(["sysctl", "-n", "kern.boottime"], check=True, capture_output=True, text=True)
            boot_time_str = result.stdout.split("=")[1].split(",")[0].strip()
            boot_time = datetime.fromtimestamp(int(boot_time_str), tz=timezone.utc)
        elif platform.system() == "Windows":  # Windows
            result = subprocess.run(["systeminfo"], check=True, capture_output=True, text=True)
            boot_time = None
            for line in result.stdout.splitlines():
                if "Boot Time" in line:
                    boot_time_str = line.split(":", 1)[1].strip()
                    boot_time = datetime.strptime(boot_time_str, "%m/%d/%Y, %I:%M:%S %p").replace(tzinfo=timezone.utc)
                    break
            if boot_time is None:
                raise ValueError("Boot Time not found in systeminfo output")
        else:
            click.echo(click.style("❓ Unsupported platform for checking last reboot time", fg="yellow"))
            return True  # Not a failure, just not applicable

        if boot_time < datetime.now(tz=timezone.utc) - timedelta(days=MAX_DAYS_SINCE_REBOOT):
            click.echo(
                click.style(
                    f"❓ Last reboot time: {boot_time}, this was more than {MAX_DAYS_SINCE_REBOOT} days ago, please consider rebooting",  # noqa: E501
                    fg="yellow",
                )
            )
        else:
            click.echo(click.style(f"✅ Last reboot time: {boot_time}", fg="green"))

        return True
    except Exception as e:
        click.echo(click.style(f"❌ Failed to get last reboot time: {e}", fg="red"))
        return False


def run_dbt_debug() -> bool:
    """Run dbt debug command."""
    click.echo(click.style("...running dbt debug...", fg="blue"))
    transform_path = projects_path() / "data-model" / "transform"
    result = subprocess.run(["dbt", "debug"], cwd=transform_path, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        click.echo(click.style("✅ dbt debug ran successfully", fg="green"))
        return True
    else:
        click.echo(click.style("❌ dbt debug failed, consider running dbt deps to fix", fg="red"))
        return False


def check_pyenv_installation() -> bool:
    """Check if pyenv is installed and meets version requirements."""
    if not shutil.which("pyenv"):
        click.echo(click.style("❌ pyenv is not installed", fg="red"))
        return False

    try:
        result = subprocess.run(["pyenv", "--version"], capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError:
        click.echo(click.style("❌ Failed to get pyenv version", fg="red"))
        return False

    version_match = re.search(r"(\d+\.\d+\.\d+)", result.stdout)
    if not version_match:
        click.echo(click.style("❌ Unable to parse pyenv version", fg="red"))
        return False

    version_str = version_match.group(1)
    version = tuple(map(int, version_str.split(".")))

    if platform.system() == "Windows" and version < MINIMUM_REQUIRED_PYENV_VERSION_WINDOWS:
        click.echo(
            click.style(
                f"❌ pyenv {version_str} is installed, "
                f"but version {'.'.join(map(str, MINIMUM_REQUIRED_PYENV_VERSION_WINDOWS))} "
                f"or higher is required",
                fg="red",
            )
        )
        return False
    elif platform.system() == "Darwin" and version < MINIMUM_REQUIRED_PYENV_VERSION_MAC:
        click.echo(
            click.style(
                f"❌ pyenv {version_str} is installed, "
                f"but version {'.'.join(map(str, MINIMUM_REQUIRED_PYENV_VERSION_MAC))} or higher is required",
                fg="red",
            )
        )
        return False
    click.echo(
        click.style(
            f"✅ pyenv {version_str} is installed "
            f"(>={'.'.join(map(str, MINIMUM_REQUIRED_PYENV_VERSION_WINDOWS if platform.system() == 'Windows' else MINIMUM_REQUIRED_PYENV_VERSION_MAC))})",  # NOQA: E501
            fg="green",
        )
    )
    return True
