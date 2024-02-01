import subprocess
from contextlib import suppress
from pathlib import Path

import click
from cookiecutter.main import cookiecutter


@click.group()
def cli() -> None:
    pass


def internal_package_path() -> Path:
    if is_root():
        return Path("packages")
    return Path("../../packages")


def projects_path() -> Path:
    if is_root():
        return Path("projects")
    return Path("../../projects")


def template_path() -> Path:
    if is_root():
        return Path("templates")
    return Path("../../templates")


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

    if name in list_dirs_in(internal_package_path()):
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


@cli.command()
def build() -> None:
    name = folder_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo(
            "An error occured while trying to get the project name. "
            "Make sure you run this command from a project directory.",
            err=True,
        )
        return

    echo_action(f"Building project '{name}'")
    subprocess.run(["docker", "compose", "build"])


def is_module(module_name: str) -> bool:
    path = module_name.replace(".", "/").split(":")[0] + ".py"
    return Path(path).is_file()


@cli.command()
@click.argument("subcommand", nargs=-1)
def run(subcommand: str) -> None:
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
    cwd = Path().cwd().resolve().as_posix()
    commands = ["docker", "compose", "run", "-v", f"{cwd}/:/opt/{name}", "--service-ports", name]
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
def up() -> None:
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
    command = ["docker", "compose", "up"]
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
def new(type_name: str) -> None:
    echo_action(f"Creating new {type_name}")

    git_user_name = git_config("user.name")
    git_user_email = git_config("user.email")
    git_user_username = git_config("user.username")

    extra_context = {}

    if git_user_name:
        extra_context["owner_full_name"] = git_user_name
    if git_user_email:
        extra_context["owner_email_address"] = git_user_email
    if git_user_username:
        extra_context["owner_github_handle"] = git_user_username

    click.echo(f"Found extra context: {extra_context}")

    if type_name == "project":
        cookiecutter(
            (template_path() / "project").as_posix(),
            output_dir=projects_path().as_posix(),
            extra_context=extra_context,
        )
    elif type_name == "package":
        cookiecutter(
            (template_path() / "project").as_posix(),
            output_dir=projects_path().as_posix(),
            extra_context=extra_context,
        )


@cli.command()
def test() -> None:
    echo_action("Testing project")
    command = ["pytest", "-rav", "tests"]
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
