from typing import Optional, Union
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    for query in constraint_sqls:
        print(f"Executing query: {query}")
        try:
            spark.sql(query)
        except Exception as e:
            print(f"❌ Failed to execute: {query}\nError: {e}")
            success = False
    
    if success:
        print(f"✅ Completed: {table}")
    else:
        raise Exception(f"⚠️ Failed to complete all queries for: {table}")
    

def create_or_replace_tables(
    source_database: str,
    source_schema: str,
    sink_database: str,
    sink_schema: str,
    tables: Optional[Union[list[str], str]] = None,
    excluded_tables: Optional[Union[list[str], str]] = None,
    max_workers: int = 4
) -> None:
    """
    Copies one or more tables from one schema to another in Databricks.
    If a table already exists in the sink schema, it will be overwritten.

    Args:
        source_database (str): Source database.
        source_schema (str): Source schema.
        sink_database (str): Destination database.
        sink_schema (str): Destination schema.
        tables (list of str, or a comma-separated string, optional): A list of tables to copy. If no tables are provided, all tables in the source schema will be copied.
        excluded_tables (list of str, or a comma-separated string, optional): Tables to exclude from copying. 
        max_workers (int, optional): Maximum number of threads to use for copying tables. Default is 4.
    """
    spark = SparkSession.builder.getOrCreate()

    if excluded_tables:
        if isinstance(excluded_tables, str):
            excluded_tables_set: set[str] = {
                t.strip() for t in excluded_tables.split(",") if t.strip()
            }
        elif isinstance(excluded_tables, list):
            excluded_tables_set = set(excluded_tables)
        else:
            raise ValueError(
                "excluded_tables must be a list of strings or a comma-separated string."
            )
    else:
        excluded_tables_set = set()

    if tables:
        if isinstance(tables, str):
            table_list = [t.strip() for t in tables.split(",") if t.strip()]
        elif isinstance(tables, list):
            table_list = tables
        else:
            raise ValueError("tables must be a list of strings or a comma-separated string.")
    else:
        all_tables = spark.sql(f"SHOW TABLES IN `{source_database}`.`{source_schema}`").collect()
        table_list = [row.tableName for row in all_tables]

    # Sorting tables based on prefix
    dim_tables = [t for t in table_list if t.startswith("dim") and t not in excluded_tables_set]
    rest_tables = [t for t in table_list if not t.startswith("dim") and t not in excluded_tables_set]

    def run_in_parallel(tables_to_run: list[str]) -> None:
        success = True
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(create_or_replace_table, source_database, source_schema, sink_database, sink_schema, table): table for table in tables_to_run
            }

            for future in as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Error when copying {table}: {e}")
                    success = False
        if not success:
            raise Exception("Copy of one or more tables failed.")
    
    success = True
    print("🔷 Starting copy of dim tables first ...")
    try:
        run_in_parallel(dim_tables)
    except Exception as e:
        print(f"⚠️ Error during dim table copy: {e}")
        success = False

    print("🔷 Starting copy of remaining tables (FACT, BRIDGE, etc.)...")
    try:
        run_in_parallel(rest_tables)
    except Exception as e:
        print(f"⚠️ Error during remaining table copy: {e}")
        success = False

    if not success:
        raise Exception("⚠️ Copy of one or more tables failed.")

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
    Copies all tables in one or more schemas from one database to another in Databricks.

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
    for schema in final_schemas:

        source_schema = f"{source_schema_prefix}{schema}"
        sink_schema = f"{sink_schema_prefix}{schema}"
        
        if source_database == sink_database and source_schema == sink_schema:
            print(f"Skipping schema: {schema}")
            continue

        print(f"Copying schema: {schema}")
        try:
            create_or_replace_tables(
                source_database=source_database,
                source_schema=source_schema,
                sink_database=sink_database,
                sink_schema=sink_schema,
                max_workers=max_workers
            )
        except Exception as e:
            print(f"❌ Error copying schema {schema}: {e}")
            success = False
    if not success:
        raise Exception("⚠️ Copy of one or more schemas failed.")
            