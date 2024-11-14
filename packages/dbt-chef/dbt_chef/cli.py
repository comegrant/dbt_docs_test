#TODO: Update readme based on this
#TODO: Add a way to automatically update the docs and yaml files

import subprocess
from pathlib import Path

import click


@click.group()
def cli() -> None:
    """CLI tool for local dbt development."""

def is_in_transform_directory() -> bool:
    if Path.cwd().name != 'transform':
        click.echo("Error: This command must be run from the 'transform' directory.")
        return False
    return True

def create_silver_directory(source_system: str) -> None:
    Path(f"models/silver/{source_system}").mkdir(parents=True, exist_ok=True)

@cli.command()
@click.option('--source-system', required=True, help='Name of the source system, e.g. cms')
@click.option('--source-table-name', required=True, help='Name of the source table, e.g. cms__company')
@click.option('--model-name', required=True, help='Name of the dbt model to be created, e.g. cms__companies')
def generate_silver_model(source_system: str, source_table_name: str, model_name: str) -> None:
    """Generate SQL file for a silver layer model."""
    if not is_in_transform_directory():
        return

    create_silver_directory(source_system)
    generate_silver_model_sql_file(source_system, source_table_name, model_name)
    click.echo(f"SQL file for silver layer model '{model_name}' created successfully")

@cli.command()
@click.option('--model-name', required=True, help='Name of the dbt model')
def generate_docs(model_name: str) -> None:
    """Generate column documentation for any model."""
    if not is_in_transform_directory():
        return

    build_model(model_name)
    generate_column_docs(model_name)
    click.echo(f"Column documentation for '{model_name}' generated successfully")

@cli.command()
@click.option('--model-name', required=True, help='Name of the dbt model')
def generate_yaml(model_name: str) -> None:
    """Generate column YAML for any model."""
    if not is_in_transform_directory():
        return

    build_model(model_name)
    generate_column_yaml(model_name)
    click.echo(f"Column YAML for '{model_name}' generated successfully")

def generate_silver_model_sql_file(source_system: str, source_table_name: str, model_name: str) -> None:
    click.echo(f"Generating the SQL file models/silver/{source_system}/{model_name}.sql")
    output_file = f"models/silver/{source_system}/{model_name}.sql"
    sql_command = (
        f'dbt run-operation --quiet generate_silver_model_sql '
        f'--args \"{{\'source_name\': \'{source_system}\', \'source_table_name\': \'{source_table_name}\'}}\" '
        f'> {output_file}'
    )
    subprocess.run(sql_command, shell=True, check=True)

def build_model(model_name: str) -> None:
    click.echo(f"Building the model {model_name}")
    build_command = f"dbt build -s {model_name}"
    subprocess.run(build_command, shell=True, check=True)

def generate_column_docs(model_name: str) -> None:
    click.echo(f"Generating column documentation for {model_name}")
    docs_command = f'dbt run-operation generate_column_docs --args "model_name: {model_name}"'
    subprocess.run(docs_command, shell=True, check=True)
    click.echo("⬆️  Copy the above output and paste it into the relevant __docs.md file for this model ⬆️")

def generate_column_yaml(model_name: str) -> None:
    click.echo(f"Generating column YAML for {model_name}")
    yaml_command = f'dbt run-operation generate_column_yaml --args "model_name: {model_name}"'
    subprocess.run(yaml_command, shell=True, check=True)
    click.echo("⬆️  Copy the above output and paste it into the relevant __models.yml folder ⬆️")

if __name__ == '__main__':
    cli()
