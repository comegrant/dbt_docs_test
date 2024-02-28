import json
import logging

logger = logging.getLogger()


class DatabaseSparkConnector:
    def __init__(self, db: str, spark, dbutils, local: bool = False):
        self.db = db
        self.dbutils = dbutils
        self.spark = spark
        self.local = local

        self.get_connection_properties()

    def get_local_conf(self):
        if self.db == "mldb":
            return None, None
        elif self.db == "analyticsdb":
            return None, None
        elif self.db == "postgresdb":
            return None, None
        else:
            logger.info("db %s not found", self.db)
            return None, None

    def get_conf(self, db_settings: str = "/dbfs/FileStore/tables/dwh_settings.json"):
        """Fetch the database configuration variables stored on databricks file storage"""
        with open(db_settings) as filename:
            conf = json.load(filename)

        if self.db == "mldb":
            return (
                conf["write_db"],
                self.dbutils.secrets.get(scope="DataBricksScope", key="MLAnalyticsDBPassword"),
            )

        elif self.db == "analyticsdb":
            return (
                conf["read_db"],
                self.dbutils.secrets.get(scope="DataBricksScope", key="AnalyticsDBPassword"),
            )
        elif self.db == "postgresdb":
            return (
                conf["postgres_read_db"],
                self.dbutils.secrets.get(scope="DataBricksScope", key="PostgresDBPassword"),
            )

        else:
            logger.info(f"db {self.db} not found")
            return None, None

    def get_connection_properties(self):
        """Set the connection properties"""
        if self.local:
            conf_db, jdbc_password = self.get_local_conf()
        else:
            conf_db, jdbc_password = self.get_conf()

        jdbc_hostname = conf_db["host"]
        jdbc_database = conf_db["dbname"]
        jdbc_port = conf_db["port"]
        jdbc_username = conf_db["user"]
        jdbc_driver = conf_db["driver"]
        self.connection_properties = {
            "user": jdbc_username,
            "password": jdbc_password,
            "driver": jdbc_driver,
        }

        if self.db != "postgresdb":
            self.jdbcUrl = "jdbc:sqlserver://{}:{};database={}".format(
                jdbc_hostname,
                jdbc_port,
                jdbc_database,
            )
        else:
            self.jdbcUrl = "jdbc:postgresql://{}:{}/{}".format(
                jdbc_hostname,
                jdbc_port,
                jdbc_database,
            )

    def load_table_to_sql(self, schema: str, table_name: str):
        """Load table from database into spark sql context"""
        df = self.spark.read.jdbc(
            self.jdbcUrl,
            f"{schema}.{table_name}",
            properties=self.connection_properties,
        )
        df.createOrReplaceTempView(table_name)
        return df

    def extract_from_db(self, query):
        """Extract data from database"""
        return self.spark.read.jdbc(self.jdbcUrl, query, properties=self.connection_properties)

    def execute_jdbc_query(self, query):
        """Execute query in database"""
        driver_manager = self.spark._sc._gateway.jvm.java.sql.DriverManager
        con = driver_manager.getConnection(
            self.jdbcUrl,
            self.connection_properties["user"],
            self.connection_properties["password"],
        )
        stmt = con.createStatement()
        logger.info("Executing query %s...", query[:100])
        stmt.executeUpdate(query)
        stmt.close()

    def delete_from_table(self, ids, schema, table_name):
        logger.info("Deleting ids from table")
        placeholders = ", ".join(f"'{id}'" for id in ids)
        if len(ids) > 1:
            query = f"DELETE FROM {schema}.{table_name} WHERE id in (%s)" % placeholders
        else:
            query = f"DELETE FROM {schema}.{table_name} WHERE id = %s" % placeholders
        try:
            self.execute_jdbc_query(query)
        except Exception as e:
            logger.exception("Failed to delete from table %s.%s: %s", schema, table_name, e)

    def truncate_tables(self, schema, tables):
        for table in tables:
            query = f"TRUNCATE TABLE {schema}.{table}"
        try:
            self.execute_jdbc_query(query)
        except Exception as e:
            logger.exception("Failed to truncate table %s.%s: %s", schema, table, e)

    def insert_to_db(
        self,
        df,
        schema,
        table_name,
        collapse_partitions=False,
        tablelock: str = "true",
        batchsize: str = "1048576",
    ):
        try:
            logger.info("Number of partitions: %s", df.rdd.getNumPartitions())
            if collapse_partitions:
                df = df.coalesce(1)
                logger.info("Number of partitions: %s", df.rdd.getNumPartitions())

            logger.info("Writing dataframe to table %s.%s", schema, table_name)

            count = df.count()
            logger.info("Number of rows to be inserted: %s", count)
            df.printSchema()

            if count > 100000:
                df.write.format("com.microsoft.sqlserver.jdbc.spark").mode("append").option(
                    "url",
                    self.jdbcUrl,
                ).option("dbtable", f"{schema}.{table_name}").option(
                    "username",
                    self.connection_properties["user"],
                ).option(
                    "password",
                    self.connection_properties["password"],
                ).option(
                    "tableLock",
                    tablelock,
                ).option(
                    "batchsize",
                    batchsize,
                ).save()
            else:
                df.write.format("com.microsoft.sqlserver.jdbc.spark").mode("append").option(
                    "url",
                    self.jdbcUrl,
                ).option("dbtable", f"{schema}.{table_name}").option(
                    "username",
                    self.connection_properties["user"],
                ).option(
                    "password",
                    self.connection_properties["password"],
                ).save()
        except Exception as e:
            logger.exception("Failed to write to table %s.%s: %s", schema, table_name, e)
