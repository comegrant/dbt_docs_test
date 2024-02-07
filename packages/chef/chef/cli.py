import subprocess
from collections import defaultdict
from contextlib import suppress
from importlib import import_module
from pathlib import Path

import click
import yaml
from cookiecutter.main import cookiecutter


@click.group()
def cli() -> None:
    pass


def root_dir() -> Path:
    if is_root():
        return Path(".")
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
    return Path(".").resolve().parent.name == "projects"


def folder_name() -> str | Exception:
    if not is_project():
        return ValueError("Not in a project directory")

    return Path(".").resolve().name


def internal_packages() -> list[str]:
    return list_dirs_in(internal_package_path())


def internal_projects() -> list[str]:
    return list_dirs_in(projects_path())


@cli.command()
def list_internal_deps() -> None:
    echo_action("Internal dependencies:")
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
        subprocess.run(command)
    else:
        click.echo("Looking for external package")
        command = ["poetry", "add", name]
        if extras:
            command.extend(["--extras", f"{extras}"])
        subprocess.run(command)


@cli.command()
@click.argument("name")
def remove(name: str) -> None:
    echo_action(f"Removing {name}")
    subprocess.run(["poetry", "remove", name])


@cli.command()
def install() -> None:
    echo_action("Installing dependencies")
    subprocess.run(["poetry", "install"])


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
    )


@cli.command()
@click.argument("port")
def expose(port: str) -> None:
    echo_action(f"Exposing port {port} to a public url")
    subprocess.run(["ngrok", "http", f"{port}"])


def load_compose_file(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict:
    with open(folder_path / compose_file) as file:
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
def build(project: str | None) -> None:
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
    commands.extend(["build"])
    subprocess.run(commands)


def is_module(module_name: str) -> bool:
    path = module_name.replace(".", "/").split(":")[0] + ".py"
    return Path(path).is_file()


@cli.command()
@click.argument("subcommand", nargs=-1)
def run(subcommand) -> None:
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

    subprocess.run(commands)


@cli.command()
@click.argument("project", default=None, required=False)
def up(project: str | None) -> None:
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
    ports = compose_exposed_ports(projects_path() / name)

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

    command = compose_command(projects_path() / name)
    command.extend(["up", "--remove-orphans"])
    subprocess.run(command)


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
    command = compose_command(projects_path() / name)
    command.extend(["down"])
    subprocess.run(command)


@cli.command()
def bash() -> None:
    name = folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running bash for project '{name}'")
    command = ["docker", "compose", "run", "-i", name, "bash"]
    subprocess.run(command)


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

    git_user_name = git_config("user.name")
    git_user_email = git_config("user.email")
    git_user_username = git_config("user.username")
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
    if len(python_version.split(".")) >= 3:
        python_version = ".".join(python_version.split(".")[:2])

    extra_context = {
        "python_version": python_version,
    }

    if git_user_name:
        extra_context["owner_full_name"] = git_user_name
    if git_user_email:
        extra_context["owner_email_address"] = git_user_email
    if git_user_username:
        extra_context["owner_github_handle"] = git_user_username

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
    subprocess.run(["poetry", "lock", f"--directory={output_dir}"])


@cli.command()
def test() -> None:
    echo_action("Testing project")
    command = ["pytest", "-rav", "."]
    subprocess.run(command)


@cli.command(name="format")
def format_code() -> None:
    echo_action("Formatting project")
    command = ["black", "."]
    subprocess.run(command)


@cli.command()
@click.option("--fix", default=True, is_flag=True)
def lint(fix: bool) -> None:
    echo_action("Linting project")
    command = ["ruff", "."]
    if fix:
        command.append("--fix")

    subprocess.run(command)


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

    with open(env_file, "w") as file:
        file.write(env_file)


if __name__ == "__main__":
    cli()
