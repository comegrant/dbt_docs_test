"""Main CLI entry point for the chef package."""

import asyncio
import functools
import subprocess
from contextlib import suppress
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path

import click
from cookiecutter.main import cookiecutter
from project_owners.owner import Owner, owner_for_email

from chef.dbt_commands import register_dbt_commands
from chef.deploy import Apps, deploy
from chef.docker_utils import (
    compose_command,
    compose_exposed_ports,
    compose_services,
    load_compose_file,
)
from chef.doctor_checks import (
    check_docker_installation,
    check_git_branch_format,
    check_git_configuration,
    check_last_rebase,
    check_last_reboot,
    check_poetry_installation,
    check_project,
    check_pyenv_installation,
    check_python_version,
    check_user_in_owners,
    check_vscode_databricks_extension,
    run_dbt_debug,
)
from chef.package_workflow import write_package_workflow
from chef.powerbi_commands import register_powerbi_commands
from chef.project_workflow import deploy_project_workflow, test_project_workflow
from chef.utils import (
    compose_path,
    echo_action,
    folder_name,
    git_config,
    internal_packages,
    internal_projects,
    is_module,
    path_for_internal_package,
    projects_path,
    set_git_config,
    should_prompt_user,
    template_path,
    workflow_dir,
)

logger = None  # Will be set if needed


def coro(func):  # noqa
    """Decorator to run async functions."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # noqa
        return asyncio.run(func(*args, **kwargs))

    return wrapper


@click.group()
def cli() -> None:
    """Chef CLI - Your kitchen assistant for development."""


# Register DBT commands
register_dbt_commands(cli)

# Register PowerBI commands
register_powerbi_commands(cli)


# Package Management Commands
@cli.command()
def list_packages() -> None:
    """List all internal packages."""
    echo_action("Listing internal packages")
    for package in internal_packages():
        click.echo(f"- {package}")


@cli.command()
def list_projects() -> None:
    """List all projects."""
    echo_action("Listing projects")
    for project in internal_projects():
        click.echo(f"- {project}")


@cli.command()
@click.argument("name")
@click.option("--extras", default=None)
def add(name: str, extras: str | None) -> None:
    """Add a package to the current project."""
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
    """Remove a package from the current project."""
    echo_action(f"Removing {name}")
    subprocess.run(["poetry", "remove", name], check=False)


@cli.command()
def install() -> None:
    """Install project dependencies."""
    echo_action("Installing dependencies")
    subprocess.run(["poetry", "install"], check=False)


@cli.command()
@click.argument("name")
@click.option("--version", default=None)
@click.option("--repo", default=None)
def pin(name: str, version: str | None, repo: str | None) -> None:
    """Pin an internal package to a specific version."""
    if name not in internal_packages():
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


# Docker Commands
@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--profile-or-service", default=None, required=False)
def build(project: str | None, profile_or_service: str | None) -> None:
    """Build a project's Docker containers."""
    name = project or folder_name()

    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    if name not in internal_projects():
        click.echo("-------------------------------")
        click.echo("Available internal projects:")
        for package in internal_projects():
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


@cli.command()
@click.argument("subcommand", nargs=-1)
def run(subcommand: tuple) -> None:
    """Run a project or execute a command in a project container."""
    name: str | Exception = ValueError("No project name found")

    if subcommand and subcommand[0] in internal_projects():
        name = subcommand[0]
        subcommand = subcommand[1:]
    else:
        name = folder_name()

    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
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
@click.argument("profile_or_service", default=None, required=False)
@click.option("--project", default=None, required=False)
def up(profile_or_service: str | None, project: str | None) -> None:
    """Start a project's Docker containers."""
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    path = compose_path(name)
    echo_action(f"Running project '{name}'")

    compose = load_compose_file(path)

    services = compose_services(
        compose,
        profile=profile_or_service,
        service_name=profile_or_service,
    )

    command = compose_command(path)
    command.append("up")
    command.extend(services)
    command.append("--remove-orphans")

    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
def down(project: str | None) -> None:
    """Stop a project's Docker containers."""
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Stopping project '{name}'")
    command = compose_command(compose_path(name))
    command.extend(["down"])
    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--service", default=None, required=False)
def shell(project: str | None, service: str | None) -> None:
    """Open a shell in a project's Docker container."""
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Opening shell for project '{name}'")

    if not service:
        service = name

    command = compose_command(compose_path(name))
    command.extend(["run", "-i", service, "bash"])
    subprocess.run(command, check=False)


# Project Management Commands
@cli.command()
@click.argument("type_name", type=click.Choice(["project", "package"]))
def create(type_name: str) -> None:
    """Create a new project or package."""
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

    extra_context = {
        "python_version": python_version,
        "owner_full_name": info.name,
        "owner_email_address": info.email,
        "owner_slack_handle": info.slack_member_id,
    }

    click.echo("Setting default variables:")
    for key, value in extra_context.items():
        click.echo(f"- {key}: {value}")

    output_dir: str | None = None

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
                test_project_workflow(
                    project_name,
                    pr_file.resolve().relative_to(Path().resolve()).as_posix(),
                    python_version=extra_context["python_version"],
                )
            )
            deploy_file.write_text(
                deploy_project_workflow(project_name, deploy_file.resolve().relative_to(Path().resolve()).as_posix())
            )

        elif type_name == "package":
            output_dir = cookiecutter(
                (template_path() / "package").as_posix(),
                output_dir=Path("packages").as_posix(),
                no_input=not should_prompt_user(),
                extra_context=extra_context,
                overwrite_if_exists=True,
                skip_if_file_exists=True,
            )
            assert isinstance(output_dir, str)
            package_name = output_dir.split("packages/")[-1]

            pr_file = workflow_dir() / f"{package_name}_pr.yaml"
            write_package_workflow(pr_file, Path(), package_name, extra_context["python_version"])

    except Exception as e:
        click.echo(f"\nAn error occurred while creating the {type_name}.\n{e}")
        return

    subprocess.run(["poetry", "lock", f"--directory={output_dir}"], check=False)


@cli.command()
@click.argument("project", default=None, required=False)
def lock(project: str | None) -> None:
    """Lock dependencies for a project."""
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    if name in internal_projects():
        path = projects_path() / name
    else:
        path = Path("packages") / name

    echo_action(f"Locking '{name}' {path}")
    command = ["poetry", "lock", f"--directory={path}"]
    subprocess.run(command, check=False)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--test-service", default=None, required=False)
def test(project: str | None, test_service: str | None) -> None:
    """Run tests for a project."""
    echo_action("Testing project")

    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occurred while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    if not test_service:
        test_service = f"{name}-test"

    if name not in internal_projects():
        command = compose_command(Path("packages") / name)
    else:
        command = compose_command(projects_path() / name)
    command.extend(["run", test_service])
    subprocess.run(command, check=False)


# Development Commands
@cli.command(name="format")
def format_code() -> None:
    """Format code using black."""
    echo_action("Formatting project")
    command = ["black", "."]
    subprocess.run(command, check=False)


@cli.command()
@click.option("--fix", default=True, is_flag=True)
def lint(fix: bool) -> None:
    """Lint code using ruff."""
    echo_action("Linting project")
    command = ["ruff", "check", "."]
    if fix:
        command.append("--fix")

    subprocess.run(command, check=False)


@cli.command()
@click.option("--settings-path", "-s", default="settings:Settings")
@click.option("--output-file", "-o", default=".env")
def generate_dotenv(settings_path: str, output_file: str) -> None:
    """Generate .env file from Pydantic settings."""
    echo_action("Generating .env file")

    module_path, object_name = settings_path.split(":")
    module = import_module(module_path)
    obj = getattr(module, object_name)

    env_content = ""
    for key, value in obj.model_fields.items():
        env_content += f"{key}="

        if value.default and str(value.default) != "PydanticUndefined":
            env_content += f"{value.default}"

        if value.description:
            env_content += f" # {value.description}"
        env_content += "\n"

    with Path(output_file).open("w") as file:
        file.write(env_content)


# Deployment Commands
@cli.command()
def catalog() -> None:
    """Start the data catalog."""
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
@click.option("--app", "-a", default=None, help="The app to deploy")
@click.option("--env", "-e", default="test", help="The environment to deploy to")
@click.option("--tag", "-t", default="main-latest", help="The docker tag to use")
@click.option("--manifest-module", default="apps", help="The environment to deploy to")
@coro
async def deploy_streamlit(app: str | None, env: str, manifest_module: str, tag: str) -> None:
    """Deploy a streamlit app to Azure."""
    echo_action("Deploying Streamlit app")

    env = {"production": "prod", "development": "dev"}.get(env.lower(), env.lower())
    assert env.lower() in ["dev", "test", "prod"]

    manifest = import_module(manifest_module)

    def find_config() -> Apps | None:
        for attr in dir(manifest):
            val = getattr(manifest, attr)

            if isinstance(val, Apps):
                return val
        return None

    apps = find_config()

    if apps is None:
        raise ValueError(f"Unable to find any apps in {manifest_module}")

    async def deploy_app(app: str) -> None:
        project_name = folder_name()
        if isinstance(project_name, Exception):
            raise project_name

        app_name = f"{project_name}-{app}-{env}"
        click.echo(f"Found an application to deploy '{app}' and will name it {app_name}")

        if apps.docker_image is None:
            apps.docker_image = f"bhregistry.azurecr.io/{project_name}:{tag}"

        config = apps.config_for(project_name, name=app, env=env)

        click.echo(f"Creating the application '{config.app_name}'. This may take some time")
        url = await deploy(config, env)
        click.echo(f"Successfully created an application for '{app}' which will be available on '{url}'")

    if app is not None:
        await deploy_app(app)
    else:
        click.echo(f"Deploying all apps '{apps.applications.keys()}'")
        for app_name in apps.applications:
            await deploy_app(app_name)


# Utility Commands
@cli.command()
@click.argument("port")
def expose(port: str) -> None:
    """Expose a port using ngrok."""
    echo_action(f"Exposing port {port} to a public url")
    subprocess.run(["ngrok", "http", f"{port}"], check=False)


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
    """Push a Docker image to a registry."""
    if not project:
        name = folder_name()

        if isinstance(name, Exception):
            click.echo(name)
            click.echo(
                "An error occurred while trying to get the project name. "
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


# Doctor Command
@cli.command()
@click.option("--project", default=None, help="Check a specific project")
def doctor(project: str | None) -> None:
    """Check your environment setup."""
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


# Helper Functions
def owner() -> Owner | Exception:
    """Get the current user's owner information."""
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
    """Set up user information if missing."""
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


if __name__ == "__main__":
    cli()
