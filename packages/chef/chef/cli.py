import subprocess
from pathlib import Path

import click


@click.group()
def cli() -> None:
    pass


def internal_package_path() -> Path:
    return Path("../../packages")


def internal_packages() -> list[str]:
    return [
        package.name
        for package in internal_package_path().iterdir()
        if package.is_dir()
    ]


def path_for_internal_package(name: str) -> Path:
    return internal_package_path() / name


def is_project() -> bool:
    return Path(".").resolve().parent.name == "projects"


def project_name() -> str | Exception:
    if not is_project():
        return ValueError("Not in a project directory")

    return Path(".").resolve().name


@cli.command()
def list_internal_deps() -> None:
    echo_action("Internal dependencies:")
    for package in internal_packages():
        click.echo(f"- {package}")


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
def install() -> None:
    echo_action("Installing dependencies")
    subprocess.run(["poetry", "install"])


@cli.command()
@click.argument("name")
@click.option("--version", default=None)
@click.option("--repo", default=None)
def pin(name: str, version: str | None, repo: str | None) -> None:
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
    )


@cli.command()
@click.argument("port")
def expose(port: str) -> None:
    echo_action(f"Exposing port {port} to a public url")
    subprocess.run(["ngrok", "http", f"{port}"])


@cli.command()
def build() -> None:
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Building project '{name}'")
    subprocess.run(["docker", "build", "-t", name, "-f", "Dockerfile", "../../"])


@cli.command()
@click.argument("subcommand", nargs=-1)
@click.option("--command", default="python")
def run(command: str, subcommand: str) -> None:
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running project '{name}'")
    commands = ["docker", "run", name]
    if subcommand:
        commands.append(command)
        if isinstance(subcommand, tuple):
            commands.extend(list(subcommand))
        else:
            commands.extend(subcommand.split(" "))
    subprocess.run(commands)


@cli.command()
def bash() -> None:
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Running bash for project '{name}'")
    command = ["docker", "run", "-it", name, "bash"]
    subprocess.run(command)


@cli.command()
@click.argument("type_name", type=click.Choice(["project", "package"]))
def new(type_name: str) -> None:
    echo_action(f"Creating new {type_name}")

    if type_name == "project":
        subprocess.run(["cookiecutter", "templates/project", "-o", "projects"])
    elif type_name == "package":
        subprocess.run(["cookiecutter", "templates/package", "-o", "packages"])


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


if __name__ == "__main__":
    cli()
