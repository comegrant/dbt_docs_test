from pathlib import Path
import click
import subprocess

@click.group()
def cli():
    pass


def internal_package_path() -> Path:
    return Path('../../packages')

def internal_packages() -> list[str]:
    return [package.name for package in internal_package_path().iterdir() if package.is_dir()]

def path_for_internal_package(name: str) -> Path:
    return internal_package_path() / name

def is_project():
    return Path('.').resolve().parent.name == 'projects'

def project_name() -> str | Exception:
    if not is_project():
        return ValueError("Not in a project directory")

    return Path('.').resolve().name

@cli.command()
def list_internal_deps():
    echo_action('Internal dependencies:')
    for package in internal_packages():
        click.echo(f"- {package}")

def echo_action(action: str):
    click.echo(click.style(f"Yes Chef!", bold=True))
    click.echo(action)

@cli.command()
@click.argument('name')
@click.option('--extras', default=None)
def add(name: str, extras: str | None):
    echo_action(f"Adding {name}")

    if name in internal_packages():
        click.echo("Found internal package")
        path = path_for_internal_package(name)
        click.echo(f"Adding {path.as_posix()}")
        command = ['poetry', 'add', '--editable', path.as_posix()]
        if extras:
            command.extend(['--extras', f'{extras}'])
        subprocess.run(command)
    else:
        click.echo("Looking for external package")
        command = ['poetry', 'add', name]
        if extras:
            command.extend(['--extras', f'{extras}'])
        subprocess.run(command)


@cli.command()
def install():
    echo_action("Installing dependencies")
    subprocess.run(['poetry', 'install'])


@cli.command()
def setup():
    echo_action("Setting up Python environment")
    subprocess.run(['pip', 'install', 'poetry'])
    subprocess.run(['poetry', 'install'])
    subprocess.run(['pre-commit', 'install'])


@cli.command()
@click.argument('name')
@click.option('--version', default=None)
@click.option('--repo', default=None)
def pin(name: str, version: str | None, repo: str | None):

    if name not in internal_packages():
        click.echo(f"Package '{name}' not found as an internal package")
        return

    if not repo:
        click.echo(f"No repo was set, will resolve to origin remote.")
        repo_url = subprocess.check_output([
            'git', 'remote', 'get-url', 'origin'
        ]).decode('utf-8').strip()
        repo = repo_url.replace(":", "/")

    if not version:
        click.echo(f"No version was set, will resolve to latest commit on main branch.")
        version = subprocess.check_output(['git', 'rev-parse', 'origin/main']).decode('utf-8').strip()

    click.echo(f"Found repo url: {repo}")
    click.echo(f"Pinning {name} to {version}")

    path = path_for_internal_package(name).as_posix().replace("../../", "")

    subprocess.run([
        'poetry', 'add', f'git+ssh://{repo}@{version}#subdirectory={path}'
    ])


@cli.command()
@click.argument('port')
def expose(port: str):
    echo_action(f"Exposing port {port} to a public url")
    subprocess.run(['ngrok', 'http', f'{port}'])

@cli.command()
def build():
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo("An error occured while trying to get the project name. Make sure you run this command from a project directory.", err=True)
        return

    echo_action(f"Building project '{name}'")
    subprocess.run(['docker', 'build', '-t', name, '-f', 'Dockerfile', '../../'])

@cli.command()
@click.argument('subcommand', nargs=-1)
def run(subcommand: str):
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo("An error occured while trying to get the project name. Make sure you run this command from a project directory.", err=True)
        return

    echo_action(f"Running project '{name}'")
    command = ['docker', 'run', name]
    if subcommand:
        if isinstance(subcommand, tuple):
            command.extend(list(subcommand))
        else:
            command.extend(subcommand.split(' '))
    subprocess.run(command)


@cli.command()
def bash():
    name = project_name()
    if isinstance(name, Exception):
        click.echo(name)
        click.echo("An error occured while trying to get the project name. Make sure you run this command from a project directory.", err=True)
        return

    echo_action(f"Running terminal for project '{name}'")
    command = ['docker', 'run', '-it', name, 'bash']
    subprocess.run(command)


@cli.command()
def train():
    pass

if __name__ == "__main__":
    cli()
