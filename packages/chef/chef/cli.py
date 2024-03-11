import logging
import subprocess
from collections import defaultdict
from contextlib import suppress
from datetime import datetime
from importlib import import_module
from pathlib import Path

import click
import yaml
from cookiecutter.main import cookiecutter
from project_owners.owner import Owner, owner_for_email

logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    pass


def root_dir() -> Path:
    if is_root():
        return Path()
    if Path("../.git").is_dir():
        return Path("../")
    return Path("../../")


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


def compose_exposed_ports(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict[str, list[str]]:
    compose = load_compose_file(folder_path, compose_file)

    all_service_ports: dict[str, list[str]] = defaultdict(list)

    for service, content in compose["services"].items():
        for port in content.get("ports", []):
            localhost_port, _ = port.split(":")
            all_service_ports[service].append(localhost_port)

    return all_service_ports


@cli.command()
@click.argument("project", default=None, required=False)
@click.argument("profile", default="app", required=False)
def build(project: str | None, profile: str) -> None:
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
    commands = compose_command(projects_path() / name)
    commands.extend(["--profile", profile, "build"])
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
def push_image(registry: str, project: str | None, tag: str | None, image: str | None) -> None:
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
        tag = f"push-{datetime.utcnow().timestamp()}"

    url = f"{registry}/{project}:{tag}"
    echo_action(f"Pushing image '{image}' to {url}")

    path = projects_path() / project
    command = compose_command(path)
    command.extend(["build", project])
    subprocess.run(command, check=True)

    subprocess.run(["docker", "tag", image, url], check=False)
    subprocess.run(["docker", "push", url], check=False)


@cli.command()
@click.argument("project", default=None, required=False)
@click.option("--profile", default="app", required=False)
def up(project: str | None, profile: str) -> None:
    name = project or folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    path = compose_path(name)
    echo_action(f"Running project '{name}'")
    ports = compose_exposed_ports(path)

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
    command.extend(["--profile", profile, "up", "--remove-orphans"])
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


def git_config(key: str) -> str | None:
    try:
        return (
            subprocess.check_output(
                [
                    "git",
                    "config",
                    key,
                ],
            )
            .decode("utf-8")
            .strip()
        )
    except subprocess.CalledProcessError:
        return None


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

    extra_context = {
        "python_version": python_version,
    }

    extra_context["owner_full_name"] = info.name
    extra_context["owner_email_address"] = info.email
    extra_context["owner_slack_handle"] = info.slack_member_id

    click.echo("Setting default variables:")
    for key, value in extra_context.items():
        click.echo(f"- {key}: {value}")

    output_dir: str | None = None

    if type_name == "project":
        output_dir = cookiecutter(
            (template_path() / "project").as_posix(),
            output_dir=projects_path().as_posix(),
            extra_context=extra_context,
        )
    elif type_name == "package":
        output_dir = cookiecutter(
            (template_path() / "package").as_posix(),
            output_dir=internal_package_path().as_posix(),
            extra_context=extra_context,
        )
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

    echo_action(f"Locking project '{name}'")
    command = ["poetry", "lock", f"--directory={projects_path() / name}"]
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


if __name__ == "__main__":
    cli()
