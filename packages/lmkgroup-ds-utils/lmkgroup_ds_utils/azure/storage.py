import logging
import os
import random
import shutil
import string
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from azure.storage.blob import BlobServiceClient
from databricks.sdk import WorkspaceClient
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger("ds_utils.azure.storage")
load_dotenv(find_dotenv())


class BlobConnector:
    def __init__(self, local: bool = False) -> None:
        self.w = WorkspaceClient()
        self.dbutils = self.w.dbutils
        try:
            if local:
                account_key = os.getenv("DATALAKE_STORAGE_ACCOUNT_KEY")
                tenant_id = os.getenv("AZURE_TENANT_ID")
                client_id = os.getenv("DATALAKE_SERVICE_PRINCIPAL_CLIENT_ID")
                client_secret = os.getenv("DATALAKE_SERVICE_PRINCIPAL_CLIENT_SECRET")
                account_url = os.getenv("DATALAKE_SERVICE_ACCOUNT_URL")
            else:
                account_key = None
                account_url = "https://gganalyticsdatalake.blob.core.windows.net/"
                tenant_id = (self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-DirID"),)
                client_id = (self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-AppID"),)
                client_secret = self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-Secret")

            if account_key is not None:
                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=account_key,
                )
            elif client_id is not None and client_secret is not None and tenant_id is not None:
                from azure.identity import ClientSecretCredential

                token = ClientSecretCredential(
                    tenant_id=self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-DirID"),
                    client_id=self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-AppID"),
                    client_secret=self.dbutils.secrets.get(scope="BlobStorage", key="DataLake-Secret"),
                )

                self.blob_service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=token,
                )
            else:
                logger.info("Missing arguments to make connection!")
        except Exception as exception:
            logger.exception(exception)

    def list_blobs(self, path: str = "/", container: str = "data-science") -> list:
        """
        List files existing on a container
        Keyword arguments:
        path -- the folder to search on (default /)

        eg: list_dir(container="test", path="churn-ai/GL/data/interm/")
        """
        container_client = self.blob_service_client.get_container_client(container=container)
        files = []
        for blob in container_client.list_blobs(name_starts_with=path):
            files.append(blob.name)
        return files

    def upload_df(
        self,
        dataframe: pd.DataFrame,
        file_path: str,
        filename: str,
        container: str = "data-science",
    ) -> None:
        """
        Upload DataFrame to Azure Blob Storage to given container
        Keyword arguments:
        container -- name of the container
        dataframe -- the dataframe(df) object (default None)
        file_path -- path folder to store the dataframe
        filename -- the filename to use for the blob (default None)

        eg: upload_df(container="test", dataframe=df, file_path="", filename="test.csv")
        """
        upload_file_path = Path(file_path) / filename
        logger.info(f"Uploading to {upload_file_path} in container {container}")
        blob_client = self.blob_service_client.get_blob_client(
            container=container,
            blob=str(upload_file_path),
        )
        try:
            output = dataframe.to_csv(index=False, encoding="utf-8")
        except Exception as exception:
            logger.exception("Failed to save dataframe: %s", exception)
            return
        try:
            blob_client.upload_blob(data=output, blob_type="BlockBlob", overwrite=True)
        except Exception as exception:
            logger.exception("Failed to save dataframe: %s", exception)

    def download_file(self, filename: str, blob: str, container: str = "data-science") -> None | BytesIO:
        """
        Download dataframe from Azure Blob Storage for given url
        Keyword arguments:
        url -- the url of the blob (default None)

        Function uses following enviornment variables
        AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
        eg: download_file(filename=<file_name>, blob=<blob_name>, container=<container_name>)
        """

        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        with BytesIO() as input_blob:
            try:
                blob_client.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                with Path.open(filename, "wb") as f:
                    f.write(input_blob.getbuffer())
                    return f
            except Exception as exception:
                logger.exception("Failed to download file: %s", exception)
                return None

    def download_csv_to_df(self, blob: str, container: str = "data-science", **kwargs: int) -> pd.DataFrame | None:
        """
        Download dataframe from Azure Blob Storage for given url
        Keyword arguments:
        url -- the url of the blob (default None)

        Function uses following enviornment variables
        AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
        eg: download_csv_to_df(blob=<blob_name>, container=<container_name>)
        """
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        with BytesIO() as input_blob:
            try:
                blob_client.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob, **kwargs)
                return df
            except Exception as exception:
                logger.exception("Failed to download dataframe: %s", exception)
                return None

    def download_json_to_df(self, blob: str, container: str = "data-science", **kwargs: int) -> pd.DataFrame | None:
        """
        Download dataframe from Azure Blob Storage for given json url
        Keyword arguments:
        url -- the url of the blob (default None)
        Function uses following enviornment variables
        AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
        eg: download_json_to_df(blob=<blob_name>, container=<container_name>)
        """
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)
        with BytesIO() as input_blob:
            try:
                blob_client.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_json(input_blob, **kwargs)
                return df
            except Exception as exception:
                logger.exception("Failed to download dataframe: %s", exception)
                return None

    def upload_local_file(
        self,
        container: str,
        local_file_path: str,
        remote_file_path: str,
        filename: str,
    ) -> None:
        """
        Upload local file to Azure Blob Storage
        Keyword arguments:
        container -- the container name (default None)
        local_file_path -- path of source file
        remote_file_path -- path of target file
        filename -- the filename to use for the blob (default None)

        eg: upload_local_file(
            container="test",
            local_file_path="/tmp/file.csv",
            remote_file_path="dbfs:/folder/",
            filename="test.csv")
        """
        upload_file_path = Path(remote_file_path) / filename

        logger.info(f"Creating blob client {upload_file_path} in container {container}")
        blob_client = self.blob_service_client.get_blob_client(
            container=container,
            blob=str(upload_file_path),
        )
        try:
            with Path.open(local_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
        except Exception as exception:
            logger.exception("Failed to upload file: %s", exception)

    def delete_file(self, url: str | None = None) -> None:
        """
        Deletes dataframe on Azure Blob Storage for given url
        Keyword arguments:
        url -- the url of the blob (default None)

        Function uses following enviornment variables
        AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
        eg: download_delete_file("https://<account_name>.blob.core.windows.net/<container_name>/<blob_name>")
        """
        if url:
            path = urlparse(url).path
            path = path.split("/")
            container = path[1]
            blob = "/".join(path[2:])
            blob_client = self.blob_service_client.get_blob_client(container=container, blob=blob)

            try:
                blob_client.delete_blob()
            except Exception as exception:
                logger.exception("Failed to download dataframe: %s", exception)


class BlobMount:
    def __init__(self) -> None:
        self.mounts: dict[str, str] = {}

    def add_mounted_path(self, path: str, storage_account: str, container: str) -> None:
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net"
        if source not in self.mounts:
            self.mounts[source] = path
        else:
            logger.info("The source {source} is already mounted.")

    def mount_storage(
        self,
        storage_account: str,
        container: str,
        scope: str,
        secret: str,
        path: str,
    ) -> None:
        """
        Mount a storage to the DBFS filesystem.
        Keyword arguments:
        storage_account -- the storage account name
        container -- the container name (default None)
        scope -- the scope for the secret
        secret -- the secret stored in the scope, for accessing the storage
        path -- the mounting folder for the storage
        """
        try:
            source = f"wasbs://{container}@{storage_account}.blob.core.windows.net"

            if source in self.mounts:
                logger.info("Source already is mounted on %s", self.mounts[source])
                return

            self.w.dbutils.fs.mount(
                source=source,
                mount_point=path,
                extra_configs={
                    f"fs.azure.account.key.{storage_account}.blob.core.windows.net": self.w.dbutils.secrets.get(
                        scope=scope,
                        key=secret,
                    ),
                },
            )

            self.mounts[source] = path
        except Exception as e:
            logger.exception("Error mounting the storage: %s", e)

    def unmount_storage(self, path: str) -> None:
        """
        Unmount a storage from the DBFS filesystem.
        Keyword arguments:
        path -- the mounting folder for the storage
        """
        try:
            self.w.dbutils.fs.unmount(path)
            self.mounts = {key: val for key, val in self.mounts.items() if val != path}
        except Exception as e:
            logger.exception("Error unmounting storage: %s", e)

    def list_mounts(self) -> None:
        """
        List all the storage mounts in the DBFS filesystem.
        Keyword arguments:
        """
        for key, val in self.mounts.items():
            logger.info("%s -> %s", key, val)

    def copy_file(self, file: str, path: str) -> None:
        """
        Copy a file from the local filesystem to a mounted storage.
        Keyword arguments:
        file -- the file path
        path -- the mounting folder for the storage, including the folder where its gonna be stored
        """
        filename = Path.name(file)
        random_folder_name = "".join(random.choice(string.ascii_lowercase) for i in range(5))
        tmp_folder = Path("tmp", random_folder_name)
        old_dbfs = Path("/dbfs", tmp_folder)
        new_dbfs = Path("dbfs:", tmp_folder)

        Path.mkdir(old_dbfs, parents=True)
        try:
            shutil.copyfile(file, Path(old_dbfs, filename))
            self.w.dbutils.fs.cp(Path(new_dbfs, filename), path)
        except Exception as e:
            logger.exception("Failed to copy file: %s", e)
        finally:
            shutil.rmtree(old_dbfs)

    def copy_folder(self, folder: str, path: str) -> None:
        """
        Copy a folder from the local filesystem to a mounted storage.
        Keyword arguments:
        folder -- the folder path
        path -- the mounting folder for the storage, including the folder where its gonna be stored
        """
        folder_name = Path.name(folder)
        random_folder_name = "".join(random.choice(string.ascii_lowercase) for i in range(5))
        tmp_folder = Path("tmp", random_folder_name)
        old_dbfs = Path("/dbfs", tmp_folder)
        new_dbfs = Path("dbfs:", tmp_folder)

        try:
            shutil.copytree(folder, Path(old_dbfs, folder_name))
            self.w.dbutils.fs.cp(Path(new_dbfs, folder_name), path, recurse=True)
        except Exception as e:
            logger.exception("Failed to copy folder: %s", e)
        finally:
            shutil.rmtree(old_dbfs)
