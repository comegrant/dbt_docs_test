from pathlib import Path

import yaml


def read_yaml(
    file_name: str,
    directory: str | None = None,
) -> dict:
    """Read a yaml file and return the content.
    Args:
        file_name (str): the file name without extension
        directory (str, optional): yaml file directory. Defaults to None.
    Returns:
        Dict: yaml content as a dictionary
    """
    yaml_path = f"{directory}/{file_name}.yml"

    with Path(yaml_path).open() as f:
        yaml_dict = yaml.safe_load(f)
    return yaml_dict
