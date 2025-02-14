import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path

import click
import yaml
from cookiecutter.main import cookiecutter
from project_owners.owner import Owner, owner_for_email

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
from chef.project_workflow import deploy_project_workflow, test_project_workflow

logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    pass


def init_dotenv(project: str) -> None:
    root_env = root_dir() / ".env"
    project_env = projects_path() / project / ".env"

    if not root_env.exists():
        root_env.touch()

    if not project_env.exists():
        project_env.touch()


def root_dir() -> Path:
    # Find the mono_repo directory containing the .git folder
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
    return root_dir() / ".github" / "workflows"


def internal_package_path() -> Path:
    return root_dir() / "packages"


def projects_path() -> Path:
    return root_dir() / "projects"


def template_path() -> Path:
    return root_dir() / "templates"


def list_dirs_in(path: Path) -> list[str]:
    return [package.name for package in path.iterdir() if package.is_dir()]


def path_for_internal_package(name: str) -> Path:
    return internal_package_path() / name


def is_root() -> bool:
    return Path(".git").is_dir()


def is_project() -> bool:
    return Path().resolve().parent.name == "projects"


def folder_name() -> str | Exception:
    return Path().resolve().name


def internal_packages() -> list[str]:
    return list_dirs_in(internal_package_path())


def internal_projects() -> list[str]:
    return list_dirs_in(projects_path())


def is_existing_package(name: str) -> bool:
    import importlib

    try:
        importlib.import_module(name)
        return True
    except ImportError:
        return False


def compose_path(name: str) -> Path:
    if name in internal_projects():
        return projects_path() / name
    return internal_package_path() / name


@cli.command()
def list_packages() -> None:
    echo_action("Listing internal packages")
    for package in list_dirs_in(internal_package_path()):
        click.echo(f"- {package}")


@cli.command()
def list_projects() -> None:
    echo_action("Listing projects")
    for project in list_dirs_in(projects_path()):
        click.echo(f"- {project}")


def echo_action(action: str) -> None:
    click.echo(click.style("Yes Chef!", bold=True))
    click.echo(action)


def owner() -> Owner | Exception:
    email = git_config("user.email")

    if not email:
        return ValueError("Missing user.email in git config")

    owner = owner_for_email(email)
    if owner:
        return owner

    return ValueError(
        f"No owner found for email: '{email}'. "
        "Please add your info to the project-owners.owner:Owner.all_owners() function.",
    )


def setup_user_info() -> None:
    info = owner()
    if not isinstance(info, Exception):
        click.echo("✅ User info is complete")
        return

    if "name" in info.args[0]:
        name = click.prompt("What is your name?")
        set_git_config("user.name", name)

    if "email" in info.args[0]:
        email = click.prompt("What is your email?")
        set_git_config("user.email", email)


@cli.command()
@click.argument("name")
@click.option("--extras", default=None)
def add(name: str, extras: str | None) -> None:
    echo_action(f"Adding {name}")

    if name in internal_packages():
        click.echo("Found internal package")
        path = path_for_internal_package(name)
        click.echo(f"Adding {path.as_posix()}")
        command = ["poetry", "add", "--editable", path.as_posix()]
        if extras:
            command.extend(["--extras", f"{extras}"])
        subprocess.run(command, check=False)
    else:
        click.echo("Looking for external package")
        command = ["poetry", "add", name]
        if extras:
            command.extend(["--extras", f"{extras}"])
        subprocess.run(command, check=False)


@cli.command()
@click.argument("name")
def remove(name: str) -> None:
    echo_action(f"Removing {name}")
    subprocess.run(["poetry", "remove", name], check=False)


@cli.command()
def install() -> None:
    echo_action("Installing dependencies")
    subprocess.run(["poetry", "install"], check=False)


@cli.command()
@click.argument("name")
@click.option("--version", default=None)
@click.option("--repo", default=None)
def pin(name: str, version: str | None, repo: str | None) -> None:
    if name not in list_dirs_in(internal_package_path()):
        click.echo(f"Package '{name}' not found as an internal package")
        return

    if not repo:
        click.echo("No repo was set, will resolve to origin remote.")
        repo_url = (
            subprocess.check_output(
                [
                    "git",
                    "remote",
                    "get-url",
                    "origin",
                ],
            )
            .decode("utf-8")
            .strip()
        )
        repo = repo_url.replace(":", "/")

    if not version:
        click.echo("No version was set, will resolve to latest commit on main branch.")
        version = (
            subprocess.check_output(
                [
                    "git",
                    "rev-parse",
                    "origin/main",
                ],
            )
            .decode("utf-8")
            .strip()
        )

    click.echo(f"Found repo url: {repo}")
    click.echo(f"Pinning {name} to {version}")

    path = path_for_internal_package(name).as_posix().replace("../../", "")

    subprocess.run(
        [
            "poetry",
            "add",
            f"git+ssh://{repo}@{version}#subdirectory={path}",
        ],
        check=False,
    )


@cli.command()
@click.argument("port")
def expose(port: str) -> None:
    echo_action(f"Exposing port {port} to a public url")
    subprocess.run(["ngrok", "http", f"{port}"], check=False)


def load_compose_file(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict:
    with (folder_path / compose_file).open() as file:
        return yaml.safe_load(file)


def compose_command(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> list[str]:
    return ["docker", "compose", "-f", (folder_path / compose_file).as_posix()]


def compose_services(
    compose: dict,
    profile: str | None = None,
    service_name: str | None = None,
) -> list[str]:
    all_services = []

    for service, content in compose["services"].items():
        if profile and profile in content.get("profiles", []):
            all_services.append(service)

        if service_name and service_name == service:
            all_services.append(service)

    if not all_services and (profile or service_name):
        raise ValueError(
            f"Unable to find service with profile '{profile}' or name '{service_name}'",
        )

    return all_services


def compose_content_exposed_ports(
    compose: dict,
    service_names: list[str] | None = None,
) -> dict[str, list[str]]:
    all_service_ports: dict[str, list[str]] = defaultdict(list)

    for service, content in compose["services"].items():
        if service_names and service not in service_names:
            continue

        for port in content.get("ports", []):
            localhost_port, _ = port.split(":")
            all_service_ports[service].append(localhost_port)

    return all_service_ports


def compose_exposed_ports(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict[str, list[str]]:
    compose = load_compose_file(folder_path, compose_file)

    return compose_content_exposed_ports(compose)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--profile-or-service", default=None, required=False)
def build(project: str | None, profile_or_service: str | None) -> None:
    name = project or folder_name()

    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    internal_projects = list_dirs_in(projects_path())
    if name not in internal_projects:
        click.echo("-------------------------------")
        click.echo("Available internal projects:")
        for package in internal_projects:
            click.echo(f"- {package}")
        click.echo("-------------------------------")
        click.echo(f"Unable to find project '{name}'", err=True)
        return

    echo_action(f"Building project '{name}'")
    compose = load_compose_file(projects_path() / name)
    services = compose_services(
        compose,
        profile=profile_or_service,
        service_name=profile_or_service,
    )

    commands = compose_command(projects_path() / name)
    commands.append("build")

    if services:
        commands.extend(services)

    subprocess.run(commands, check=False)


def is_module(module_name: str) -> bool:
    path = module_name.replace(".", "/").split(":")[0] + ".py"
    return Path(path).is_file()


@cli.command()
@click.argument("subcommand", nargs=-1)
def run(subcommand: tuple) -> None:
    name: str | Exception = ValueError("No project name found")

    if subcommand and subcommand[0] in internal_projects():
        name = subcommand[0]
        subcommand = subcommand[1:]
    else:
        name = folder_name()

    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running project '{name}'")
    project_dir = projects_path() / name
    commands = compose_command(project_dir)

    commands.extend(
        [
            "run",
            "-v",
            f"{project_dir.resolve().as_posix()}/:/opt/projects/{name}",
            "--service-ports",
            name,
        ],
    )

    if subcommand:
        with suppress(Exception):
            first_arg = subcommand[0]

            if Path(first_arg).is_file():
                commands.append("python")
            elif is_module(first_arg):
                commands.append("python")
                commands.append("-m")
            elif first_arg == "streamlit":
                commands.extend(["streamlit", "run"])
                subcommand = subcommand[1:]

        if isinstance(subcommand, tuple):
            commands.extend(list(subcommand))
        else:
            commands.extend(subcommand.split(" "))

    if "streamlit" in commands:
        click.echo("---------------------------------")
        click.echo("Spinning up streamlit app")
        click.echo("Press Ctrl+C to stop the app")
        click.echo("Streamlit app at: http://127.0.0.1:8501")
        click.echo("---------------------------------")

    subprocess.run(commands, check=False)


@cli.command()
@click.option("--registry", default="bhregistry.azurecr.io", required=False)
@click.option("--project", default=None, required=False)
@click.option("--tag", default=None, required=False)
@click.option("--image", default=None, required=False)
def push_image(
    registry: str,
    project: str | None,
    tag: str | None,
    image: str | None,
) -> None:
    if not project:
        name = folder_name()

        if isinstance(name, Exception):
            click.echo(name)
            click.echo(
                "An error occured while trying to get the project name. "
                "Make sure you run this command from a project directory.",
                err=True,
            )
            return

        project = name

    if not image:
        image = f"{project}-{project}"

    if not tag:
        tag = f"push-{datetime.now(tz=timezone.utc).timestamp()}"

    url = f"{registry}/{project}:{tag}"
    echo_action(f"Pushing image '{image}' to {url}")

    path = projects_path() / project
    command = compose_command(path)
    command.extend(["build", project, "--build-arg", f"TAG={tag}"])

    subprocess.run(command, check=True)

    subprocess.run(["docker", "tag", image, url], check=False)
    subprocess.run(["docker", "push", url], check=False)


@cli.command()
@click.argument("profile_or_service", default=None, required=False)
@click.option("--project", default=None, required=False)
def up(profile_or_service: str | None, project: str | None) -> None:
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    init_dotenv(project=name)
    path = compose_path(name)
    echo_action(f"Running project '{name}'")

    compose = load_compose_file(path)

    services = compose_services(
        compose,
        profile=profile_or_service,
        service_name=profile_or_service,
    )
    ports = compose_content_exposed_ports(compose, service_names=services)

    if ports:
        click.echo("")
        click.echo("-------------------------------")
        click.echo("")
        click.echo("Services are exposed on the following urls:")

        for service, exposed_ports in ports.items():
            click.echo("")
            click.echo(f"{service}")
            for port in exposed_ports:
                click.echo(f"- http://127.0.0.1:{port}")
        click.echo("")
        click.echo("-------------------------------")
        click.echo("")

    command = compose_command(path)
    command.append("up")
    command.extend(services)
    command.append("--remove-orphans")

    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
def down(project: str | None) -> None:
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running project '{name}'")
    command = compose_command(compose_path(name))
    command.extend(["down"])
    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--service", default=None, required=False)
def shell(project: str | None, service: str | None) -> None:
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running bash for project '{name}'")

    if not service:
        service = name

    command = compose_command(compose_path(name))
    command.extend(["run", "-i", service, "bash"])
    subprocess.run(command, check=False)


def set_git_config(key: str, value: str) -> None:
    subprocess.run(["git", "config", "--global", key, value], check=False)


def read_command(command: list[str]) -> str | None:
    try:
        return subprocess.check_output(command).decode("utf-8").strip()
    except subprocess.CalledProcessError:
        return None


def git_config(key: str) -> str | None:
    return read_command(["git", "config", key])


def default_create_context() -> dict[str, str]:
    "This is only used to inject test variables"
    return {}


def should_prompt_user() -> bool:
    return True


@cli.command()
@click.argument("type_name", type=click.Choice(["project", "package"]))
def create(type_name: str) -> None:
    echo_action(f"Creating new {type_name}")

    minor_version_location = 3

    info = owner()
    if isinstance(info, Exception):
        setup_user_info()
        info = owner()

        if isinstance(info, Exception):
            raise info

    python_version = (
        subprocess.check_output(
            [
                "python",
                "--version",
            ],
        )
        .decode("utf-8")
        .replace("Python ", "")
        .strip()
    )
    if len(python_version.split(".")) >= minor_version_location:
        python_version = ".".join(python_version.split(".")[:2])

    extra_context = default_create_context()
    extra_context["python_version"] = python_version
    extra_context["owner_full_name"] = info.name
    extra_context["owner_email_address"] = info.email
    extra_context["owner_slack_handle"] = info.slack_member_id

    click.echo("Setting default variables:")
    for key, value in extra_context.items():
        click.echo(f"- {key}: {value}")

    output_dir: str | None = None

    root_folder = root_dir()

    try:
        if type_name == "project":
            click.echo("Creating project")
            output_dir = cookiecutter(
                (template_path() / "project").as_posix(),
                output_dir=projects_path().as_posix(),
                no_input=not should_prompt_user(),
                extra_context=extra_context,
                overwrite_if_exists=True,
                skip_if_file_exists=True,
                accept_hooks=False,
            )
            assert isinstance(output_dir, str)
            project_name = output_dir.split("projects/")[-1]

            pr_file = workflow_dir() / f"{project_name}_pr.yaml"
            deploy_file = workflow_dir() / f"{project_name}_deploy.yaml"
            pr_file.write_text(
                test_project_workflow(project_name, pr_file.resolve().relative_to(root_folder.resolve()).as_posix())
            )
            deploy_file.write_text(
                deploy_project_workflow(
                    project_name, deploy_file.resolve().relative_to(root_folder.resolve()).as_posix()
                )
            )

        elif type_name == "package":
            output_dir = cookiecutter(
                (template_path() / "package").as_posix(),
                output_dir=internal_package_path().as_posix(),
                no_input=not should_prompt_user(),
                extra_context=extra_context,
                overwrite_if_exists=True,
                skip_if_file_exists=True,
            )
    except Exception as e:
        click.echo(f"\nAn error occured while creating the {type_name}.\n{e}")
        return

    subprocess.run(["poetry", "lock", f"--directory={output_dir}"], check=False)


@cli.command()
@click.argument("project", default=None, required=False)
def lock(project: str | None) -> None:
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    if name in internal_projects():
        path = projects_path() / name
    else:
        path = internal_package_path() / name

    echo_action(f"Locking '{name}' {path}")
    command = ["poetry", "lock", f"--directory={path}"]
    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--test-service", default=None, required=False)
def test(project: str | None, test_service: str | None) -> None:
    echo_action("Testing project")

    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    if not test_service:
        test_service = f"{name}-test"

    if name not in internal_projects():
        command = compose_command(internal_package_path() / name)
    else:
        command = compose_command(projects_path() / name)
    command.extend(["run", test_service])
    subprocess.run(command, check=False)


@cli.command(name="format")
def format_code() -> None:
    echo_action("Formatting project")
    command = ["black", "."]
    subprocess.run(command, check=False)


@cli.command()
@click.option("--fix", default=True, is_flag=True)
def lint(fix: bool) -> None:
    echo_action("Linting project")
    command = ["ruff", "check", "."]
    if fix:
        command.append("--fix")

    subprocess.run(command, check=False)


@cli.command()
@click.option("--settings-path", "-s", default="settings:Settings")
@click.option("--output-file", "-o", default=".env")
def generate_dotenv(settings_path: str, env_file: str) -> None:
    """
    This assumes that the `pydantic` is installed.
    """
    echo_action("Generating .env file")

    module_path, object_name = settings_path.split(":")
    module = import_module(module_path)
    obj = getattr(module, object_name)

    env_file = ""
    for key, value in obj.model_fields.items():
        env_file += f"{key}="

        if value.default and str(value.default) != "PydanticUndefined":
            env_file += f"{value.default}"

        if value.description:
            env_file += f" # {value.description}"
        env_file += "\n"

    with Path(env_file).open("w") as file:
        file.write(env_file)


@cli.command()
def catalog() -> None:
    echo_action("Spinning up the catalog")

    catalog_path = projects_path() / "data-catalog"
    compose_file = "docker-compose.yaml"

    ports = compose_exposed_ports(catalog_path, compose_file)
    for service, exposed_ports in ports.items():
        click.echo(f"Service: {service}")
        for port in exposed_ports:
            click.echo(f"- http://127.0.0.1:{port}")

    command = compose_command(catalog_path, compose_file)
    command.extend(["run", "--service-ports", "data-catalog-app"])
    subprocess.run(command, check=False)


@cli.command()
@click.option("--project", default=None, help="Check a specific project")
def doctor(project: str | None) -> None:
    echo_action("Checking your environment setup")

    sections = [
        (
            "System Checks",
            [
                check_python_version,
                check_poetry_installation,
                check_pyenv_installation,
                check_docker_installation,
                check_vscode_databricks_extension,
                check_last_reboot,
            ],
        ),
        ("Git Checks", [check_git_configuration, check_last_rebase, check_git_branch_format, check_user_in_owners]),
    ]

    if project:
        project_checks = [lambda: check_project(project)]
        if project == "data-model":
            project_checks.append(run_dbt_debug)
        sections.append(("Project Checks", project_checks))

    all_passed = True
    for section_name, checks in sections:
        click.echo(f"\n{click.style(section_name, bold=True)}:")
        for check in checks:
            passed = check()
            all_passed = all_passed and passed
        click.echo()  # Add an empty line after each section

    if all_passed:
        click.echo(click.style("✅ All checks passed! Your environment is set up correctly.", fg="green"))
    else:
        click.echo(click.style("❌ Some checks failed. Please address the issues above.", fg="red"))


def check_python_version() -> bool:
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
            f"✅ Poetry {version_str} is installed " f"(>={'.'.join(map(str, MINIMUM_REQUIRED_POETRY_VERSION))})",
            fg="green",
        )
    )
    return True


def check_docker_installation() -> bool:
    if shutil.which("docker"):
        click.echo(click.style("✅ Docker is installed", fg="green"))
        return True
    else:
        click.echo(click.style("❌ Docker is not installed", fg="red"))
        return False


def check_git_configuration() -> bool:
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


if __name__ == "__main__":
    cli()
