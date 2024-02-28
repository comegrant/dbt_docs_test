"""
Frontend upstash:
- Upstash is a serverless data for redis and kafka that includes a rest api and javascript sdk

Example of inputs
key_cols = ["agreement_id", "yearweek"]
score_col = "order_of_relevance_cluster"
id_col = "product_id"
"""

import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd
from dotenv import find_dotenv, load_dotenv

logger = logging.getLogger()
try:
    from redis import Redis
except ImportError:
    logger.error("Redis not installed! Please install it via the extra variable")


load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)


class RedisOnlineStore:
    """Online store config for Redis store"""

    def __init__(self, params=None):
        self.db_host, self.db_password, self.db_port = self._load_environment_vars(params)
        self._client = self._get_client()

    def _get_client(self):
        """
        Creates the Redis client RedisCluster or Redis depending on configuration
        """
        return Redis(host=self.db_host, port=self.db_port, password=self.db_password)

    def _load_dict_vars(self, params):
        """
        Reads variable from databricks secrets
        """
        return params["RedisDBHost"], params["RedisDBPassword"], params["RedisDBPort"]

    def _load_local_vars(self):
        """
        Reads local environment variable
        """
        return (
            os.getenv("REDIS_DB_HOST"),
            os.getenv("REDIS_DB_PW"),
            os.getenv("REDIS_DB_PORT"),
        )

    def _load_environment_vars(self, params) -> None:
        """
        Loads the environment variables needed to connect to the database
        """
        try:
            return self._load_dict_vars(params)
        except Exception as exception:
            # If the dbutil fails, try fetching from local machine
            logger.info(
                "Failed loading dict vars with exception %s, try to load from local...",
                exception,
            )
            return self._load_local_vars()

    def _redis_key(self, group_keys: list) -> str:
        """
        Define the redis key that is a join between different group keys
        """
        return ":".join(map(str, group_keys))

    def _redis_set_values(self, df_group: pd.DataFrame, score_col: str, id_col: str) -> dict:
        """
        Create the value dictionary of sets
        """
        values = {}
        for _, row in df_group.iterrows():
            values[row[id_col]] = row[score_col]
        return values

    def delete_keys(self, prefix):
        """
        Delete keys for a specific prefix
        """
        number_keys_deleted = 0
        for key in self._client.scan_iter(prefix):
            self._client.delete(key)
            number_keys_deleted = number_keys_deleted + 1

        logger.info("In total %s keys deleted", number_keys_deleted)

    def update(
        self,
    ) -> None:
        """
        Look for join_keys (list of entities) that are not in use anymore
        (usually this happens when the last feature view that was using specific compound key is deleted)
        and remove all features attached to this "join_keys".
        """
        NotImplementedError()

    def _get_features_for_entity(
        self,
        # values: List[ByteString],
        # feature_view: str,
        # requested_features: List[str],
    ) -> None:
        NotImplementedError()


class RedisScoreStore(RedisOnlineStore):
    """Online store for score specific tables"""

    def write_batch(
        self,
        data: pd.DataFrame,
        key_cols: list,
        score_col: str,
        id_col: str,
        progress: Callable[[int], Any] | None = None,
    ) -> None:
        """
        Write a data of a larger batch to redis using sorted set
        """
        with self._client.pipeline(transaction=False) as pipe:
            for group_keys, df_group in data.groupby(key_cols):
                keys = self._redis_key(group_keys)
                values = self._redis_set_values(df_group, score_col, id_col)
                pipe.zadd(keys, values)
            results = pipe.execute()
            if progress:
                progress(len(results))

    def online_read(self, entity_keys: list, min_value: int, max_value: int) -> list:
        """
        Reads the scores of a table
        """
        keys = []
        for entity_key_dct in entity_keys:
            entity_values = list(entity_key_dct.values())
            keys.append(self._redis_key(entity_values))

        with self._client.pipeline(transaction=False) as pipe:
            for redis_key in keys:
                pipe.zrangebyscore(redis_key, min_value, max_value)
            results = pipe.execute()
        return results


class RedisFeatureStore(RedisOnlineStore):
    """Online store config for feature store"""

    def write_batch(
        self,
        data: list,
        feature_view: str,
        progress: Callable[[int], Any] | None = None,
    ) -> None:
        """
        Write feature values in batch to store
        """

        ts_key = f"_ts:{feature_view}"
        with self._client.pipeline(transaction=False) as pipe:
            timestamp = datetime.utcnow().timestamp()
            entity_hset = {}
            entity_hset[ts_key] = timestamp

            for (
                key,
                values,
            ) in data:  # Needs a rewrite once we know the shape of input data
                for feature_name, val in values.items():
                    feature_key = f"{feature_view}:{feature_name}"
                    entity_hset[feature_key] = val

                pipe.hmset(key, mapping=entity_hset)

            results = pipe.execute()
            if progress:
                progress(len(results))

    def online_read(
        self,
        entity_keys: list,
        feature_table: str,
        requested_features: list,
    ) -> list:
        """
        Read values of a table for a specific feature
        """
        hset_keys = [f"{feature_table}:{k}:" for k in requested_features]

        keys = []
        for entity_key in entity_keys:
            keys.append(self._redis_key(entity_key))

        with self._client.pipeline(transaction=False) as pipe:
            for redis_key in keys:
                pipe.hmget(hset_keys, redis_key)
            results = pipe.execute()
        return results
