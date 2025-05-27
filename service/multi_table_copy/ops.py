"""
Multi-table copy operations module for cross-database data transfer.

This module provides functionality for parallel copying of multiple tables between databases
with configurable chunk sizes and filtering options. Currently supports PostgreSQL as the target database.

Key components:
- TableMapping: Configuration for individual table copy operations
- MultiTableCopyConfig: Main configuration schema for the copy operation
- split_tables: Splits table mappings into individual copy tasks
- copy_single_table: Handles the copying of a single table
- save_to_dwh_copy_method: Performs the actual data transfer using COPY command


Fields for table_mappings:
Required fields:
- source_schema: Source schema name
- source_table: Source table name
- target_schema: Target schema name
- target_table: Target table name

Optional fields:
- custom_where: Custom WHERE clause (without WHERE keyword)
- columns: List of specific columns to copy

Example how to fill resource config in dagster launchpad:

resources:
  db_config:
    config:
      chunk_size: 500000
      source_credential_key: dwh
      source_database: postgres
      source_db_type: postgresql
      source_host: dwh.jooble.com
      table_mappings:
        - source_schema: "dimension"
          source_table: "countries"
          target_schema: "dwh_test"
          target_table: "countries"
          custom_where: "id IN (2,3,4,5,6)"
          columns:
           - "id"
           - "name_country_eng"
           - "alpha_2"
        - source_schema: "dimension"
          source_table: "info_currency"
          target_schema: "dwh_test"
          target_table: "info_currency"
      target_credential_key: dwh
      target_database: postgres
      target_db_type: postgresql
      target_host: dwh.jooble.com
      truncate_target: false

"""

from typing import List
from io import StringIO
import pandas as pd
from pydantic import Field as PydanticField, ConfigDict
from dagster import (
    job,
    op,
    Config,
    DynamicOut,
    DynamicOutput,
    Field,
    make_values_resource
)
from sqlalchemy import text

from utility_hub.core_tools import generate_job_name
from utility_hub.connection_managers import DatabaseConnectionManager


def save_to_dwh_copy_method(
    context,
    schema,
    table_name,
    df: pd.DataFrame,
    conn_params: dict
):
    """
    Saves data from a DataFrame to a specified table in the database using COPY method.

    Args:
        context: The execution context object providing logging capabilities
        schema: Target schema name in the database
        table_name: Target table name in the database
        df: pandas DataFrame containing the data to be copied
        conn_params: Dictionary containing database connection parameters:
            - host: database host address
            - database: database name
            - credential_key: key for accessing credentials
            - db_type: type of database (e.g., 'postgresql')

    Raises:
        Exception: If any error occurs during the copy process, including:
            - Connection errors
            - Permission issues
            - Data format mismatches
            - Column ordering issues
    """
    db_manager = DatabaseConnectionManager()
    engine = db_manager.get_sqlalchemy_engine(**conn_params)
    connection = engine.raw_connection()

    query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position;
            """  # get column names in original order
    try:
        with connection.cursor() as cur:
            cur.execute(query)
            columns = cur.fetchall()
            column_order = [column[0] for column in columns]

            df = df.reindex(columns=column_order)  # reorder columns to original order

            csv_string = df.to_csv(index=False)

            context.log.debug(f"Copying data to >> [{schema}.{table_name}] ...")
            cur.copy_expert(
                f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT CSV, HEADER)",
                StringIO(csv_string)
            )

        connection.commit()
        context.log.debug(f"Data successfully copied to >> [{schema}.{table_name}].")
    except Exception as e:
        context.log.error(f"Error saving data to >> [{schema}.{table_name}]: {e}")
        raise e


class TableMapping(Config):
    """Configuration for a single table mapping"""
    model_config = ConfigDict(frozen=False)  # Make the model mutable

    source_schema: str = PydanticField(description="Source schema name")
    source_table: str = PydanticField(description="Source table name")
    target_schema: str = PydanticField(description="Target schema name")
    target_table: str = PydanticField(description="Target table name")
    custom_where: str = PydanticField(
        description="Custom WHERE clause for source query. Only clause without WHERE keyword.",
        default=""
    )
    columns: List[str] = PydanticField(
        description="List of columns to copy (all if not specified)",
        default_factory=list
    )


@op(out=DynamicOut(), required_resource_keys={'db_config'})
def split_tables(context):
    """
    Split table mappings into individual copy tasks for parallel processing.

    Args:
        context: The execution context containing configuration in resources

    Yields:
        DynamicOutput: Individual table copy configurations with unique mapping keys
    """
    config = context.resources.db_config

    # Convert dict mappings to TableMapping objects
    table_mappings = [
        TableMapping(**mapping) for mapping in config["table_mappings"]
    ]

    for i, mapping in enumerate(table_mappings):
        key = f"table_{i}"
        yield DynamicOutput(
            value={
                "mapping": mapping,
                "source_conn": {
                    "host": config["source_host"],
                    "database": config["source_database"],
                    "credential_key": config["source_credential_key"],
                    "db_type": config["source_db_type"]
                },
                "target_conn": {
                    "host": config["target_host"],
                    "database": config["target_database"],
                    "credential_key": config["target_credential_key"],
                    "db_type": config["target_db_type"]
                },
                "chunk_size": config["chunk_size"],
                "truncate_target": config["truncate_target"]
            },
            mapping_key=key
        )


@op
def copy_single_table(context, table_config: dict):
    """
    Copy a single table between databases with chunked processing.

    Args:
        context: The execution context for logging and monitoring
        table_config: Dictionary containing:
            - mapping: TableMapping configuration
            - source_conn: Source database connection parameters
            - target_conn: Target database connection parameters
            - chunk_size: Number of rows to process in each chunk
            - truncate_target: Boolean flag for truncating target table

    Raises:
        Exception: If any error occurs during the copy process

    Note:
        - Supports custom column selection and WHERE clause filtering
        - Uses chunked processing to handle large tables efficiently
        - Performs target table truncation if specified
        - Uses COPY command for optimal performance
    """
    mapping = table_config["mapping"]
    source_conn = table_config["source_conn"]
    target_conn = table_config["target_conn"]
    chunk_size = table_config["chunk_size"]
    truncate_target = table_config["truncate_target"]

    db_manager = DatabaseConnectionManager()

    try:
        # Build source query
        columns_str = "*" if not mapping.columns else ", ".join(mapping.columns)
        # Build query with WHERE clause only if custom_where is not empty
        source_query = f"SELECT {columns_str} FROM {mapping.source_schema}.{mapping.source_table}"
        if mapping.custom_where:
            source_query += f" WHERE {mapping.custom_where}"

        # Get source connection
        source_engine = db_manager.get_sqlalchemy_engine(**source_conn)

        # Truncate target table if specified
        if truncate_target:
            context.log.debug(
                f"Truncating target table {mapping.target_schema}.{mapping.target_table}"
            )
            truncate_query = text(
                f"TRUNCATE TABLE {mapping.target_schema}.{mapping.target_table}"
            )
            with db_manager.get_sqlalchemy_engine(**target_conn).connect() as conn:
                conn.execute(truncate_query)
                conn.commit()

        # Read and copy data in chunks
        context.log.debug(
            f"Starting copy: {mapping.source_schema}.{mapping.source_table} -> "
            f"{mapping.target_schema}.{mapping.target_table}"
        )

        context.log.debug(f"Source query:\n{source_query}")

        for chunk_number, chunk_df in enumerate(
            pd.read_sql(
                sql=source_query,
                con=source_engine,
                chunksize=chunk_size,
                dtype_backend="pyarrow"
            ),
            start=1
        ):
            context.log.debug(f"Processing chunk {chunk_number}. Rows: {chunk_df.shape[0]}")
            if chunk_df.empty:
                context.log.debug("No data to copy")
                break

            save_to_dwh_copy_method(
                context=context,
                schema=mapping.target_schema,
                table_name=mapping.target_table,
                df=chunk_df,
                conn_params=target_conn
            )

        context.log.debug(
            f"Table copy completed: {mapping.source_schema}.{mapping.source_table} -> "
            f"{mapping.target_schema}.{mapping.target_table}"
        )

    except Exception as e:
        context.log.error(
            f"Error copying table {mapping.source_schema}.{mapping.source_table}:\n{str(e)}"
        )
        raise


@job(
    resource_defs={
        "db_config": make_values_resource(
            # Source database configuration
            source_host=Field(
                str,
                description="Source database host",
                default_value="10.0.1.65"
            ),
            source_database=Field(
                str,
                description="Source database name",
                default_value="at"
            ),
            source_credential_key=Field(
                str,
                description="Credential key for source database",
                default_value="repstat"
            ),
            source_db_type=Field(
                str,
                description="Database type (postgresql, mysql, etc.)",
                default_value="postgresql"
            ),

            # Target database configuration
            target_host=Field(
                str,
                description="Target database host",
                default_value="dwh.jooble.com"
            ),
            target_database=Field(
                str,
                description="Target database name",
                default_value="postgres"
            ),
            target_credential_key=Field(
                str,
                description="Credential key for target database",
                default_value="dwh"
            ),
            target_db_type=Field(
                str,
                description="Database type (postgresql, mysql, etc.)",
                default_value="postgresql"
            ),

            # Table mappings and options
            table_mappings=Field(
                list,
                description="List of table mappings to copy.",
                default_value=[]
            ),
            chunk_size=Field(
                int,
                description="Number of rows to process in each chunk",
                default_value=500000
            ),
            truncate_target=Field(
                bool,
                description="Whether to truncate target tables before copying",
                default_value=False
            ),
        )
    },
    name=generate_job_name("multi_table_copy"),
    tags={"service": "data_copy"},
    description="Service job for parallel copying of multiple tables between databases. "
                "Only PostgreSQL is supported as the target database.",
)
def multi_table_copy_job():
    """
    Orchestrates parallel copying of multiple tables between databases.
    Configuration is provided via resource_defs.
    """
    table_configs = split_tables()
    table_configs.map(copy_single_table)
