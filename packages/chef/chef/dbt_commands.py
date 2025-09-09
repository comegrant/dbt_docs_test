"""DBT commands for the chef CLI."""

import subprocess
from pathlib import Path

import click


def is_transform_directory() -> bool:
    """Check if running from the transform directory."""
    return Path.cwd().name == "transform"


def create_silver_directory(source_system: str) -> None:
    """Create silver directory for a source system."""
    Path(f"models/silver/{source_system}").mkdir(parents=True, exist_ok=True)


def create_powerbi_directory() -> None:
    """Create PowerBI directory."""
    Path("../../powerbi/workspace-content/Main Data Model.SemanticModel/definition/tables").mkdir(
        parents=True, exist_ok=True
    )


def generate_silver_model_sql_file(source_system: str, source_table_name: str, model_name: str) -> None:
    """Generate SQL file for a silver model."""
    click.echo(f"Generating the SQL file models/silver/{source_system}/{model_name}.sql")
    output_file = f"models/silver/{source_system}/{model_name}.sql"
    sql_command = (
        f"dbt run-operation --quiet generate_silver_model_sql "
        f"--args \"{{'source_name': '{source_system}', 'source_table_name': '{source_table_name}'}}\" "
        f"> {output_file}"
    )
    subprocess.run(sql_command, shell=True, check=True)


def run_model(model_name: str) -> None:
    """Run a dbt model."""
    click.echo(f"Run the model {model_name}")
    build_command = f"dbt run -s {model_name}"
    subprocess.run(build_command, shell=True, check=True)


def prepare_states() -> None:
    """Prepare dbt states for different environments."""
    click.echo("Updating states")

    states_paths = ["states/dev", "states/test", "states/prod"]

    for path_str in states_paths:
        path = Path(path_str)
        path.mkdir(parents=True, exist_ok=True)

    subprocess.run("dbt parse --target dev", shell=True, check=True)
    subprocess.run("cp target/manifest.json states/dev", shell=True, check=True)
    subprocess.run("dbt parse --target test", shell=True, check=True)
    subprocess.run("cp target/manifest.json states/test", shell=True, check=True)
    subprocess.run("dbt parse --target prod", shell=True, check=True)
    subprocess.run("cp target/manifest.json states/prod", shell=True, check=True)


def generate_column_docs(model_name: str) -> None:
    """Generate column documentation for a model."""
    click.echo(f"Generating column documentation for {model_name}")
    docs_command = f'dbt run-operation generate_column_docs --args "model_name: {model_name}"'
    subprocess.run(docs_command, shell=True, check=True)
    click.echo("⬆️  Copy the above output and paste it into the relevant __docs.md file for this model ⬆️")


def generate_column_yaml(model_name: str) -> None:
    """Generate column YAML for a model."""
    click.echo(f"Generating column YAML for {model_name}")
    yaml_command = f'dbt run-operation generate_column_yaml --args "model_name: {model_name}"'
    subprocess.run(yaml_command, shell=True, check=True)
    click.echo("⬆️  Copy the above output and paste it into the relevant __models.yml folder ⬆️")


def generate_powerbi_tmdl(model_name: str) -> None:
    """Generate PowerBI TMDL file for a model."""
    click.echo(f"Generating powerbi tmdl file for {model_name}")

    if "fact" in model_name:
        file_name = model_name.split("_", 1)[-1].replace("_", " ").title().rstrip("s") + " Measures"
    elif "bridge" in model_name:
        file_name = model_name.replace("_", " ").title()
    else:
        file_name = model_name.split("_", 1)[-1].replace("_", " ").title()

    output_file = f'"../../powerbi/workspace-content/Main Data Model.SemanticModel/definition/tables/{file_name}.tmdl"'
    tmdl_command = f'dbt run-operation generate_powerbi_tmdl --args "model_name: {model_name}"> {output_file}'
    subprocess.run(tmdl_command, shell=True, check=True)


def register_dbt_commands(cli_group: click.Group) -> None:
    """Register DBT commands with the CLI group."""

    @cli_group.group()
    def dbt() -> None:
        """CLI tool for local dbt development."""

    @dbt.command()
    @click.option("--source-system", required=True, help="Name of the source system, e.g. cms")
    @click.option("--source-table-name", required=True, help="Name of the source table, e.g. cms__company")
    @click.option("--model-name", required=True, help="Name of the dbt model to be created, e.g. cms__companies")
    def generate_silver_model(source_system: str, source_table_name: str, model_name: str) -> None:
        """Generate SQL file for a silver layer model."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        create_silver_directory(source_system)
        generate_silver_model_sql_file(source_system, source_table_name, model_name)
        click.echo(f"SQL file for silver layer model '{model_name}' created successfully")

    @dbt.command()
    @click.option("--model-name", required=True, help="Name of the dbt model")
    def generate_docs(model_name: str) -> None:
        """Generate column documentation for any model."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        run_model(model_name)
        generate_column_docs(model_name)
        click.echo(f"Column documentation for '{model_name}' generated successfully")

    @dbt.command()
    @click.option("--model-name", required=True, help="Name of the dbt model")
    def generate_yaml(model_name: str) -> None:
        """Generate column YAML for any model."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        run_model(model_name)
        generate_column_yaml(model_name)
        click.echo(f"Column YAML for '{model_name}' generated successfully")

    @dbt.command()
    @click.option("--model-name", required=True, help="Name of the dbt model")
    def generate_tmdl(model_name: str) -> None:
        """Generate PowerBI TMDL for any model."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        create_powerbi_directory()
        generate_powerbi_tmdl(model_name)
        click.echo(f"PowerBI tmdl for '{model_name}' generated successfully")

    @dbt.command()
    def prepare_local_development() -> None:
        """Prepare local development environment."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        click.echo("Dropping tables in personal schemas")
        drop_tables_command = 'dbt run-operation drop_tables_in_personal_schemas --args "dry_run: false"'
        subprocess.run(drop_tables_command, shell=True, check=True)
        prepare_states()

    @dbt.command()
    def update_states() -> None:
        """Update dbt states."""
        if not is_transform_directory():
            raise click.ClickException(
                "❌ This command must be run from the data-model/transform directory. "
                "Please navigate to the data-model/transform directory and try again."
            )

        prepare_states()
