from typing import Optional, Union
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

def create_or_replace_table(
    source_database: str,
    source_schema: str,
    sink_database: str,
    sink_schema: str,
    table: str
) -> None:

    """
    Copies one table from one schema to another in Databricks, and adds primary and foreign key constraints.
    If a table already exists in the sink schema, it will be overwritten.

    Args:
        source_database (str): Source database.
        source_schema (str): Source schema.
        sink_database (str): Destination database.
        sink_schema (str): Destination schema.
        table (str): Table to be copied.
    """
    spark = SparkSession.builder.getOrCreate()

    constraint_sqls = []
    constraint_sqls.append( f"CREATE OR REPLACE TABLE `{sink_database}`.`{sink_schema}`.`{table}` AS SELECT * FROM `{source_database}`.`{source_schema}`.`{table}`;")

    # Get primary key constraints from the source table
    source_pk_constraints_df = spark.sql(f"""
        SELECT
            kcu.column_name
        FROM `{source_database}`.information_schema.table_constraints AS tc
        JOIN `{source_database}`.information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE
            tc.constraint_type = 'PRIMARY KEY'
            and tc.table_schema = '{source_schema}'
            and tc.table_name = '{table}'
    """)

    # Get foreign key constraints from the source table
    source_fk_constraints_df = spark.sql(f"""
        SELECT
            kcu.column_name,
            kcu_ref.table_name AS referenced_table_name,
            kcu_ref.column_name AS referenced_column_name
        FROM `{source_database}`.information_schema.table_constraints AS tc
        JOIN `{source_database}`.information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN `{source_database}`.information_schema.referential_constraints AS rc
            ON tc.constraint_name = rc.constraint_name
        JOIN `{source_database}`.information_schema.key_column_usage AS kcu_ref
            ON rc.unique_constraint_name = kcu_ref.constraint_name
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
            and tc.table_schema = '{source_schema}'
            and tc.table_name = '{table}'
    """)

    source_pk_constraints = source_pk_constraints_df.collect()
    source_fk_constraints = source_fk_constraints_df.collect()

    pk_constraints = set()
    pk_constraints_str = ''
    fk_constraints = set()

    for row in source_pk_constraints:
        pk_constraints.add((row.column_name))
        pk_constraints_str += f"{row.column_name},"
    if pk_constraints_str:
        pk_constraints_str = pk_constraints_str[:-1] # Remove trailing comma
    pk_constraint_name = pk_constraints_str.replace(',','_')

    for row in source_fk_constraints:
        fk_constraints.add((row.column_name, row.referenced_table_name, row.referenced_column_name))

    for column in pk_constraints:
        constraint_sqls.append(f"ALTER TABLE `{sink_database}`.`{sink_schema}`.`{table}` ALTER COLUMN {column} SET NOT NULL;")
    if pk_constraint_name:
        constraint_sqls.append(f"ALTER TABLE `{sink_database}`.`{sink_schema}`.`{table}` ADD CONSTRAINT {pk_constraint_name} PRIMARY KEY ({pk_constraints_str});")

    for fk_column, ref_table, ref_column in fk_constraints:
        # Check if referenced table exists
        table_exists_df = spark.sql(f"""
            SELECT 1
            FROM `{sink_database}`.information_schema.tables
            WHERE table_schema = '{sink_schema}' AND table_name = '{ref_table}'
        """)

        if table_exists_df.count() == 0:
            print(f"⚠️ Referenced table `{sink_database}`.`{sink_schema}`.`{ref_table}` does not exist – skipping FK for {fk_column}")
            continue

        # Check if referenced column is primary key
        pk_check_df = spark.sql(f"""
            SELECT kcu.column_name
            FROM `{sink_database}`.information_schema.table_constraints AS tc
            JOIN `{sink_database}`.information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = '{sink_schema}'
              AND tc.table_name = '{ref_table}'
              AND kcu.column_name = '{ref_column}'
        """)

        if pk_check_df.count() == 0:
            print(f"⚠️ Column `{ref_column}` in `{ref_table}` is not a primary key – skipping FK for {fk_column}")
            continue

        constraint_sqls.append(f"ALTER TABLE `{sink_database}`.`{sink_schema}`.`{table}` ADD CONSTRAINT {fk_column}_{table} FOREIGN KEY ({fk_column}) REFERENCES `{sink_database}`.`{sink_schema}`.`{ref_table}`({ref_column});")

    print(f"⏳ Starting: {table}")

    success = True
    error_messages = []

    for query in constraint_sqls:
        try:
            spark.sql(query)
        except Exception as e:
            msg = f"❌ Failed to execute: {query}\nError: {e}"
            print(msg)
            error_messages.append(msg)
            success = False

    if success:
        print(f"✅ Completed: {table}")
    else:
        all_errors = "\n\n".join(error_messages)
        raise Exception(f"⚠️ Failed to complete all queries for: {table}\n\nError messages:\n{all_errors}")


def create_or_replace_view(
    source_database: str,
    source_schema: str,
    sink_database: str,
    sink_schema: str,
    view: str
) -> None:
    """
    Copies a view from one schema to another in Databricks.
    Args:
        source_database (str): Source database.
        source_schema (str): Source schema.
        sink_database (str): Destination database.
        sink_schema (str): Destination schema.
        view (str): View to be copied.
    """
    spark = SparkSession.builder.getOrCreate()

    # Get the CREATE VIEW statement
    create_view_df = spark.sql(f"SHOW CREATE TABLE `{source_database}`.`{source_schema}`.`{view}`")
    create_view_sql = create_view_df.collect()[0][0]

    # Replace all source database/schema references with sink database/schema
    pattern = rf"`{source_database}`\.`{source_schema}`"
    replacement = f"`{sink_database}`.`{sink_schema}`"
    create_view_sql = re.sub(pattern, replacement, create_view_sql)

    # Replace CREATE VIEW with CREATE OR REPLACE VIEW to handle existing views
    create_view_sql = re.sub(r"CREATE VIEW", "CREATE OR REPLACE VIEW", create_view_sql, count=1)

    try:
        spark.sql(create_view_sql)
        print(f"✅ Completed: {view} (view)")
    except Exception as e:
        msg = f"❌ Failed to execute: {create_view_sql}\nError: {e}"
        print(msg)
        raise Exception(f"⚠️ Failed to complete all queries for: {view}\n\nError messages:\n{msg}")


def create_or_replace_objects(
    source_database: str,
    source_schema: str,
    sink_database: str,
    sink_schema: str,
    objects: Optional[Union[list[str], str]] = None,
    excluded_objects: Optional[Union[list[str], str]] = None,
    max_workers: int = 4
) -> None:
    """
    Copies one or more tables and views from one schema to another in Databricks.
    If a table or view already exists in the sink schema, it will be overwritten.
    """
    spark = SparkSession.builder.getOrCreate()

    if excluded_objects:
        if isinstance(excluded_objects, str):
            excluded_objects_set: set[str] = {
                t.strip() for t in excluded_objects.split(",") if t.strip()
            }
        elif isinstance(excluded_objects, list):
            excluded_objects_set = set(excluded_objects)
        else:
            raise ValueError(
                "excluded_objects must be a list of strings or a comma-separated string."
            )
    else:
        excluded_objects_set = set()

    # Get all views
    all_views = spark.sql(f"SELECT * FROM SYSTEM.INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG = '{source_database}' AND TABLE_SCHEMA = '{source_schema}'")
    view_names = [row.table_name for row in all_views.collect() if row.table_name not in excluded_objects_set]
    # Get all tables
    all_tables = spark.sql(f"SHOW TABLES IN `{source_database}`.`{source_schema}`")
    table_names = [
        row.tableName
        for row in all_tables.collect()
        if row.tableName not in excluded_objects_set and row.tableName not in view_names
    ]

    if objects:
        if isinstance(objects, str):
            object_list = [t.strip() for t in objects.split(",") if t.strip()]
        elif isinstance(objects, list):
            object_list = objects
        else:
            raise ValueError("objects must be a list of strings or a comma-separated string.")
        table_names = [t for t in table_names if t in object_list]
        view_names = [v for v in view_names if v in object_list]

    # Sorting tables based on prefix
    dim_tables = [t for t in table_names if t.startswith("dim")]
    rest_tables = [t for t in table_names if not t.startswith("dim")]

    def run_in_parallel_tables(tables_to_run: list[str]) -> None:
        success = True
        error_messages = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(create_or_replace_table, source_database, source_schema, sink_database, sink_schema, table): table for table in tables_to_run
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    msg = f"❌ Error when copying {table}: {e}"
                    print(msg)
                    error_messages.append(msg)
                    success = False
        if not success:
            all_errors = "\n\n".join(error_messages)
            raise Exception(f"Copy of one or more tables failed.\n\nError messages:\n{all_errors}")

    def run_in_parallel_views(views_to_run: list[str]) -> None:
        success = True
        error_messages = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(create_or_replace_view, source_database, source_schema, sink_database, sink_schema, view): view for view in views_to_run
            }
            for future in as_completed(futures):
                view = futures[future]
                try:
                    future.result()
                except Exception as e:
                    msg = f"❌ Error when copying view {view}: {e}"
                    print(msg)
                    error_messages.append(msg)
                    success = False
        if not success:
            all_errors = "\n\n".join(error_messages)
            raise Exception(f"Copy of one or more views failed.\n\nError messages:\n{all_errors}")

    success = True
    error_messages = []
    print("🔷 Starting copy of dim tables first ...")
    try:
        run_in_parallel_tables(dim_tables)
    except Exception as e:
        msg = f"⚠️ Error during dim table copy: {e}"
        print(msg)
        error_messages.append(msg)
        success = False

    print("🔷 Starting copy of remaining tables (FACT, BRIDGE, etc.)...")
    try:
        run_in_parallel_tables(rest_tables)
    except Exception as e:
        msg = f"⚠️ Error during remaining table copy: {e}"
        print(msg)
        error_messages.append(msg)
        success = False

    print("🔷 Starting copy of views ...")
    try:
        run_in_parallel_views(view_names)
    except Exception as e:
        msg = f"⚠️ Error during view copy: {e}"
        print(msg)
        error_messages.append(msg)
        success = False

    if not success:
        all_errors = "\n\n".join(error_messages)
        raise Exception(f"⚠️ Copy of one or more tables or views failed.\n\nError messages:\n{all_errors}")

def create_or_replace_schemas(
    source_database: str,
    sink_database: str,
    source_schema_prefix: str,
    sink_schema_prefix: str,
    schemas: Union[list[str], str],
    excluded_schemas: Optional[list[str]] = None,
    max_workers: int = 4
) -> None:
    """
    Copies all tables and views in one or more schemas from one database to another in Databricks.

    Args:
        source_database (str): The source database.
        sink_database (str): The destination database.
        source_schema_prefix (str): The prefix to use for the source schema, e.g. `~firstname_lastname_`
        sink_schema_prefix (str): The prefix to use for the destination schema, e.g. `~firstname_lastname_`
        schemas (list of str, or a comma-separated string): A list of schemas to copy the tables from and to.
        excluded_schemas (list of str, optional): A list of schemas to exclude from copying.
        max_workers (int, optional): Maximum number of threads to use for copying tables. Default is 4.
    """
    spark = SparkSession.builder.getOrCreate()

    if schemas:
        if isinstance(schemas, str):
            schema_list = [t.strip() for t in schemas.split(",") if t.strip()]
        elif isinstance(schemas, list):
            schema_list = schemas
        else:
            raise ValueError("schemas must be a list of strings or a comma-separated string.")
    else:
        all_schemas = spark.sql(f"SHOW SCHEMAS IN `{source_database}`").collect()
        schema_list = [row.namespace for row in all_schemas]

    if excluded_schemas:
        if isinstance(excluded_schemas, str):
            excluded_schema_list = [s.strip() for s in excluded_schemas.split(",") if s.strip()]
        elif isinstance(excluded_schemas, list):
            excluded_schema_list = excluded_schemas
        else:
            raise ValueError("excluded_schemas must be a list of strings or a comma-separated string.")
    else:
        excluded_schema_list = []

    final_schemas = [schema for schema in schema_list if schema not in excluded_schema_list]

    success = True
    error_messages = []
    for schema in final_schemas:

        source_schema = f"{source_schema_prefix}{schema}"
        sink_schema = f"{sink_schema_prefix}{schema}"

        if source_database == sink_database and source_schema == sink_schema:
            print(f"Skipping schema: {schema}")
            continue

        print(f"Copying schema: {schema}")
        try:
            create_or_replace_objects(
                source_database=source_database,
                source_schema=source_schema,
                sink_database=sink_database,
                sink_schema=sink_schema,
                max_workers=max_workers
            )
        except Exception as e:
            msg = f"❌ Error copying schema {schema}: {e}"
            print(msg)
            error_messages.append(msg)
            success = False
    if not success:
        all_errors = "\n\n".join(error_messages)
        raise Exception(f"⚠️ Copy of one or more schemas failed.\n\nError messages:\n{all_errors}")
