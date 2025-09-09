import os
from pathlib import Path

from chef import cli, utils
from chef.powerbi_commands import update_active_pages
from click.testing import CliRunner


def test_update_active_pages_in_powerbi_directory() -> None:
    """Test that update_active_pages runs successfully when called from the powerbi directory."""
    # Get the root directory and construct the powerbi directory path
    root_dir = utils.root_dir()
    powerbi_dir = root_dir / "projects" / "powerbi"

    # Store the current working directory
    original_cwd = Path.cwd()

    try:
        # Change to the powerbi directory
        os.chdir(powerbi_dir)

        # Run the update_active_pages function
        updated_count, updated_reports, skipped_reports = update_active_pages()

        # The test succeeds if the function completes without raising an exception
        # We can also verify that we got reasonable return values
        assert isinstance(updated_count, int)
        assert isinstance(updated_reports, list)
        assert isinstance(skipped_reports, list)
        assert updated_count >= 0

    finally:
        # Always restore the original working directory
        os.chdir(original_cwd)


def test_fix_opening_page_fails_in_root_directory() -> None:
    """Test that the fix_opening_page command fails when run from the root directory."""
    # Get the root directory
    root_dir = utils.root_dir()

    # Store the current working directory
    original_cwd = Path.cwd()

    try:
        # Change to the root directory
        os.chdir(root_dir)

        # Create a CLI runner
        runner = CliRunner()

        # Run the fix_opening_page command from root directory
        result = runner.invoke(cli.cli, ["powerbi", "fix-opening-page"])

        # The command should fail with a ClickException
        assert result.exit_code != 0
        assert "This command must be run from the powerbi project directory" in result.output

    finally:
        # Always restore the original working directory
        os.chdir(original_cwd)
