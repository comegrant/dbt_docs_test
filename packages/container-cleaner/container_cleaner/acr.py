import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from azure.containerregistry import ContainerRegistryClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

sha_1_length = 40
default_tags = ["main-latest", "dev-latest"]


def is_sha1(value: str) -> bool:
    if len(value) != sha_1_length:
        return False

    for char in value:
        if not char.isalnum():
            return False
    return True


def is_sous_chef_project(repo_name: str) -> bool:
    if repo_name.endswith("-databricks"):
        return True

    relativ_path = Path(f"../../projects/{repo_name}")
    return relativ_path.is_dir()


@dataclass
class ImageTag:
    name: str
    updated_at: datetime


def list_repos(client: ContainerRegistryClient) -> list[str]:
    return list(client.list_repository_names())


def tags_for(repo: str, client: ContainerRegistryClient) -> list[ImageTag]:
    tags = []

    for tag in client.list_tag_properties(repo):
        tags.append(ImageTag(name=tag.name, updated_at=tag.last_updated_on))

    return tags


def tags_to_keep(tags: list[ImageTag], ttl: timedelta) -> list[ImageTag]:
    n_sha_images_to_keep = 5

    sha_images: list[ImageTag] = []
    other_images: list[ImageTag] = []

    now = datetime.now(tz=timezone.utc)

    for tag in tags:
        if tag.name in default_tags:
            other_images.append(tag)
            continue

        time_since_update = now - tag.updated_at

        if is_sha1(tag.name):
            sha_images.append(tag)
            # Not the most efficient, but oh well
            sha_images = sorted(sha_images, key=lambda tag: tag.updated_at)
            sha_images = sha_images[0 : min((n_sha_images_to_keep - 1), len(sha_images))]

        if time_since_update < ttl:
            other_images.append(tag)

    return [*sha_images, *other_images]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    url = "bhregistry.azurecr.io"
    credential = DefaultAzureCredential()
    client = ContainerRegistryClient(endpoint=url, credential=credential)

    ttl = timedelta(days=30)

    for repo in list_repos(client):
        if not is_sous_chef_project(repo):
            continue

        logging.info(f"Cleaning up for repo named: '{repo}'")
        tags = tags_for(repo, client)
        keep = tags_to_keep(tags, ttl=ttl)
        keep_tag_names = {tag.name for tag in keep}

        for tag in tags:
            if tag.name not in keep_tag_names:
                logging.info(f"Deleting '{tag.name}' from '{repo}'")
                client.delete_tag(repo, tag.name)
