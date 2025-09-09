"""Docker Compose utility functions for the chef CLI."""

from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


def load_compose_file(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict[str, Any]:
    """Load a Docker Compose file."""
    with (folder_path / compose_file).open() as file:
        return yaml.safe_load(file)


def compose_command(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> list[str]:
    """Build the Docker Compose command."""
    return ["docker", "compose", "-f", (folder_path / compose_file).as_posix()]


def compose_services(
    compose: dict[str, Any],
    profile: str | None = None,
    service_name: str | None = None,
) -> list[str]:
    """Get list of services from compose file."""
    all_services = []

    for service, content in compose["services"].items():
        if profile and profile in content.get("profiles", []):
            all_services.append(service)

        if service_name and service_name == service:
            all_services.append(service)

    if not all_services and (profile or service_name):
        raise ValueError(
            f"Unable to find service with profile '{profile}' or name '{service_name}'",
        )

    return all_services


def compose_content_exposed_ports(
    compose: dict[str, Any],
    service_names: list[str] | None = None,
) -> dict[str, list[str]]:
    """Get exposed ports from compose content."""
    all_service_ports: dict[str, list[str]] = defaultdict(list)

    for service, content in compose["services"].items():
        if service_names and service not in service_names:
            continue

        for port in content.get("ports", []):
            localhost_port, _ = port.split(":")
            all_service_ports[service].append(localhost_port)

    return all_service_ports


def compose_exposed_ports(
    folder_path: Path,
    compose_file: str = "docker-compose.yaml",
) -> dict[str, list[str]]:
    """Get exposed ports from a compose file."""
    compose = load_compose_file(folder_path, compose_file)
    return compose_content_exposed_ports(compose)
