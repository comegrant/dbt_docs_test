from pathlib import Path

import click

from powerbi_chef.update_active_page import update_active_pages


def is_powerbi_directory() -> bool:
    """
    Checks if the current directory is the PowerBI directory.
    """
    # Simple check to determine if we're in the PowerBI directory
    return Path.cwd().name.lower() == "powerbi"


@click.group()
def cli() -> None:
    """CLI tool for local PowerBI development."""


@cli.command()
def fix_opening_page() -> None:
    """Update activePageName in all pages.json files to the first page in pageOrder."""
    if not is_powerbi_directory():
        click.echo("❌ This command must be run from the powerbi project directory")
        click.echo("   Please navigate to the powerbi project directory and try again.")
        return

    updated_count, updated_reports, skipped_reports = update_active_pages()

    # Display results
    click.echo("\n🔍 PowerBI Opening Page Update 🔍")
    click.echo("=" * 40)

    if updated_reports:
        click.echo(f"\n✅ {updated_count} reports updated:")
        for report in updated_reports:
            click.echo(f"  • {report}")

    if skipped_reports:
        click.echo(f"\n⏩ {len(skipped_reports)} reports already correct:")
        for report in skipped_reports:
            click.echo(f"  • {report}")

    click.echo(f"\n🎉 Summary: {updated_count} reports updated, {len(skipped_reports)} unchanged")


if __name__ == "__main__":
    cli()
