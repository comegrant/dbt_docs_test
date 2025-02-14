from pathlib import Path

from chef import cli
from click.testing import CliRunner
from project_owners.owner import Owner
from pytest_mock import MockFixture


def test_chef_create_project(mocker: MockFixture) -> None:
    real_root = cli.root_dir()
    context = {
        "library_name": "test"
    }

    mocker.patch.object(cli, "root_dir", return_value=real_root.resolve())
    mocker.patch.object(cli, "owner", return_value=Owner.matsmoll())
    mocker.patch.object(cli, "default_create_context", return_value=context)
    mocker.patch.object(cli, "should_prompt_user", return_value=False)

    runner = CliRunner()
    with runner.isolated_filesystem() as temp_dir:

        mocker.patch.object(cli, "projects_path", return_value=Path(temp_dir) / "projects")
        mocker.patch.object(cli, "workflow_dir", return_value=Path(temp_dir) / ".github/workflows")

        result = runner.invoke(
            cli.create, ["project"]
        )
        assert result.exit_code == 0
