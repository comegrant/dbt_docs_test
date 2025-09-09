"""PowerBI commands for the chef CLI."""

import json
from pathlib import Path
from typing import Any

import click


def is_powerbi_directory() -> bool:
    """Check if the current directory is the PowerBI directory."""
    return Path.cwd().name.lower() == "powerbi"


def get_report_display_name(file_path: str) -> str:
    """Get the display name of the report from the .platform file."""
    # Determine the .platform file path from the pages.json path
    report_dir = str(Path(file_path).parent.parent.parent)
    platform_file = Path(report_dir) / ".platform"

    try:
        with platform_file.open(encoding="utf-8") as f:
            platform_data = json.load(f)
            return platform_data.get("metadata", {}).get("displayName", Path(report_dir).name.split(".")[0])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        # Fallback to the report directory name if platform file not found or invalid
        return Path(report_dir).name.split(".")[0]


def update_active_pages() -> tuple[int, list[str], list[str]]:
    """Update the activePageName in all pages.json files to match the first value in pageOrder."""
    # Find all pages.json files in the current directory (PowerBI directory)
    pages_files: list[str] = [str(path) for path in Path.cwd().rglob("pages.json")]

    updated_count: int = 0
    updated_reports: list[str] = []
    skipped_reports: list[str] = []

    for file_path in pages_files:
        try:
            # Get the display name from the .platform file
            report_name = get_report_display_name(file_path)

            # Read the file content
            data: dict[str, Any]
            with Path(file_path).open(encoding="utf-8") as f:
                data = json.load(f)

            # Process only if the file has the expected structure
            if not ("pageOrder" in data and data["pageOrder"] and isinstance(data["pageOrder"], list)):
                skipped_reports.append(f"{report_name} (invalid structure)")
                continue

            # Get current and new values
            previous_active_page_name = data.get("activePageName", "none")
            new_active_page_name = data["pageOrder"][0]

            # Only update if needed
            if previous_active_page_name != new_active_page_name:
                data["activePageName"] = new_active_page_name

                # Write the updated data back to the file in a separate try block
                try:
                    with Path(file_path).open("w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)

                    # Get page index info for reports
                    page_info = ""
                    if previous_active_page_name in data["pageOrder"]:
                        previous_page_index = data["pageOrder"].index(previous_active_page_name) + 1
                        page_info = f" (Page {previous_page_index} → Page 1)"

                    updated_reports.append(f"{report_name}{page_info}")
                    updated_count += 1
                except Exception:
                    skipped_reports.append(f"{report_name} (write error)")
            else:
                skipped_reports.append(report_name)
        except Exception:
            # Extract report name from the path as fallback
            report_dir = str(Path(file_path).parent.parent.parent)
            report_name = Path(report_dir).name.split(".")[0]
            skipped_reports.append(f"{report_name} (processing error)")

    return updated_count, updated_reports, skipped_reports


def register_powerbi_commands(cli_group: click.Group) -> None:
    """Register PowerBI commands with the CLI group."""

    @cli_group.group()
    def powerbi() -> None:
        """CLI tool for local PowerBI development."""

    @powerbi.command()
    def fix_opening_page() -> None:
        """Update activePageName in all pages.json files to the first page in pageOrder."""
        if not is_powerbi_directory():
            raise click.ClickException(
                "❌ This command must be run from the powerbi project directory. "
                "Please navigate to the powerbi project directory and try again."
            )

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
