import os
import glob
import re
import random
import string
from typing import Optional, Generator, List, Any
from datetime import datetime

import signal
import threading
import sys

from sqlalchemy import text
import pandas as pd
from io import StringIO

from dagster import DynamicOutput, DagsterUnknownResourceError, Failure

# module import
from .db_configs import (
    prod_dbs,
    repstat_dbs,
    internal_dbs,
    citus,
    cloudberry,
    new_pg
)
from .data_collections import map_country_code_to_id
from .connection_managers import DatabaseConnectionManager
from .core_tools import get_datediff
from .utils import send_formatted_message


class Operations:

    @staticmethod
    def save_dataframe_as_parquet(
            context,
            df: pd.DataFrame,
            path_to_data: str,
            sql_instance_country_query: dict[str, any],
            additional_suffix: Optional[str] = None,
            sequence_number: Optional[int] = None,
    ) -> str:
        """
        Saves a DataFrame as a parquet file, using the country identifier from
        sql_instance_country_query and appending the current date to the file name.

        Args:
            context: Dagster context for logging.
            df: DataFrame to be saved.
            path_to_data: Path where the parquet file will be saved.
            sql_instance_country_query: Dictionary containing SQL query related information including the country DB.
            additional_suffix (Optional): Additional suffix for the file name.
            sequence_number (Optional): Sequence number to append to the file name for differentiation.

        Returns:
            The file path of the saved parquet file.
        """
        db_name = sql_instance_country_query.get("db_name")
        country_code = sql_instance_country_query.get("country_code", "global")
        current_date = datetime.now().strftime("%Y_%m_%d")

        file_name_parts = [country_code, db_name, "DB"]

        if additional_suffix:
            file_name_parts.append(additional_suffix)
        file_name_parts.append(current_date)

        if sequence_number is not None:
            file_name_parts.append(str(sequence_number))

        rand_code = ''.join(random.choices(string.ascii_letters, k=5))
        file_name_parts.append(rand_code)

        file_name = "_".join(file_name_parts) + ".parquet"
        file_path = os.path.join(path_to_data, file_name)

        # Save DataFrame as parquet file
        df.to_parquet(file_path, compression="snappy", index=False)

        log_msg = f"File path: {file_path}\n"
        log_msg += f"Parquet data saved for: {db_name}"
        if additional_suffix:
            log_msg += f", suffix: {additional_suffix}"
        if sequence_number is not None:
            log_msg += f", sequence: {sequence_number}"
        context.log.debug(log_msg)

        return file_path

    @staticmethod
    def delete_files(context, path_to_data):
        files = glob.glob(path_to_data + "/*")
        for f in files:
            os.remove(f)
        context.log.info(f"The files have been deleted from: {path_to_data}")

    @staticmethod
    def generate_sql_instance(
            context,
            instance_type: str,
            query: str,
            instance_name: Optional[str] = None,
            db_name: Optional[str] = None,
            ddl_query: Optional[str] = None,
            select_query: Optional[str] = None,
            custom_mapping_key: Optional[str] = None,
            params: Optional[dict[str, any]] = None
    ):
        """
        Generates database instance configurations for executing queries across different database setups.

        Args:
            context: Dagster context for accessing resources like global settings.
            db_type (str): Type of the database, e.g., 'prod' or 'replica'.
            query (str): SQL query or stored procedure to execute.
            params (Optional[Dict[str, Any]]): Additional SQL parameters to be passed.

        Yields:
            DynamicOutput containing the database instance configuration.
        """
        try:
            # Check if 'reload_countries' exists in the globals resource
            reload_countries = context.resources.globals.get("reload_countries")
            if reload_countries is not None:
                launch_countries = {country.strip('_').lower() for country in reload_countries}
            else:
                launch_countries = None

            date_range = None
            if "reload_date_start" in context.resources.globals and "reload_date_end" in context.resources.globals:
                date_range = pd.date_range(
                    pd.to_datetime(context.resources.globals["reload_date_start"]),
                    pd.to_datetime(context.resources.globals["reload_date_end"])
                )
            if "reload_date_start" in context.resources.globals and "reload_date_end" not in context.resources.globals:
                date_range = context.resources.globals["reload_date_start"]
        except DagsterUnknownResourceError:
            context.log.debug(
                "The resource 'globals' is not configured. Assigned 'None' to launch_countries and date_range")
            launch_countries = None
            date_range = None

        match instance_type:
            case 'prod':
                for db in prod_dbs:
                    if db['country'] in launch_countries:
                        context.log.debug(f"Processing database: host={db['host']}, db_name={db['database']}")
                        yield DynamicOutput(
                            value={
                                'host': db['host'],
                                'db_name': db['database'],
                                'db_type': 'mssql',
                                'credential_key': 'prod',
                                'country_id': db['country_id'],
                                'country_code': db['country'],
                                'query': query,
                                'date_range': date_range,
                                # 'to_sqlcode': params
                            },
                            mapping_key=custom_mapping_key if custom_mapping_key else db['database']
                        )
            case 'new_pg':
                for cluster in new_pg.values():
                    for country in cluster['dbs']:
                        if country in launch_countries:
                            country_id = Operations._get_country_id(country)
                            context.log.debug(f"Processing database: host={cluster['host']}, db_name={country}")
                            if country_id:
                                yield DynamicOutput(
                                    value={
                                        'host': cluster['host'],
                                        'db_name': country.lower().strip(),
                                        'db_type': cluster['db_type'],
                                        'credential_key': cluster['credential_key'],
                                        'country_id': country_id,
                                        'ddl_query': ddl_query,
                                        'procedure_call': query,
                                        'query': select_query,
                                        'date_range': date_range,
                                        # 'to_sqlcode': params
                                    },
                                    mapping_key=custom_mapping_key if custom_mapping_key else 'procedure_' + country
                                )
            case 'citus':
                for cluster in citus.values():
                    for country in cluster['dbs']:
                        if country in launch_countries:
                            country_id = Operations._get_country_id(country)
                            context.log.debug(f"Processing database: host={cluster['host']}, db_name={country}")
                            if country_id:
                                yield DynamicOutput(
                                    value={
                                        'host': cluster['host'],
                                        'db_name': country.lower().strip(),
                                        'db_type': cluster['db_type'],
                                        'credential_key': cluster['credential_key'],
                                        'country_id': country_id,
                                        'ddl_query': ddl_query,
                                        'procedure_call': query,
                                        'query': select_query,
                                        'date_range': date_range,
                                        # 'to_sqlcode': params
                                    },
                                    mapping_key=custom_mapping_key if custom_mapping_key else 'procedure_' + country
                                )
            case 'cloudberry':
                for cluster in cloudberry.values():
                    for country in cluster['dbs']:
                        if country in launch_countries:
                            country_id = Operations._get_country_id(country)
                            context.log.debug(f"Processing database: host={cluster['host']}, db_name={country}")
                            if country_id:
                                yield DynamicOutput(
                                    value={
                                        'host': cluster['host'],
                                        'db_name': country.lower().strip(),
                                        'db_type': cluster['db_type'],
                                        'credential_key': cluster['credential_key'],
                                        'country_id': country_id,
                                        'ddl_query': ddl_query,
                                        'procedure_call': query,
                                        'query': select_query,
                                        'date_range': date_range,
                                        # 'to_sqlcode': params
                                    },
                                    mapping_key=custom_mapping_key if custom_mapping_key else 'procedure_' + country
                                )
            case 'repstat':
                # Create a list of (country, cluster_info) pairs from all clusters
                all_countries = []
                for cluster in repstat_dbs.values():
                    cluster_info = {
                        'host': cluster['host'],
                        'db_type': cluster['db_type'],
                        'credential_key': cluster['credential_key']
                    }
                    for country in cluster['dbs']:
                        if country in launch_countries:
                            all_countries.append((country, cluster_info))

                # Shuffle the combined list of countries
                from random import shuffle
                shuffle(all_countries)

                # Process countries in shuffled order
                for country, cluster_info in all_countries:
                    country_id = Operations._get_country_id(country)
                    context.log.debug(f"Processing database: host={cluster_info['host']}, db_name={country}")
                    if country_id:
                        yield DynamicOutput(
                            value={
                                'host': cluster_info['host'],
                                'db_name': country.lower().strip(),
                                'db_type': cluster_info['db_type'],
                                'credential_key': cluster_info['credential_key'],
                                'country_id': country_id,
                                'ddl_query': ddl_query,
                                'procedure_call': query,
                                'query': select_query,
                                'date_range': date_range,
                            },
                            mapping_key=custom_mapping_key if custom_mapping_key else 'procedure_' + country
                        )
            case 'internal':
                if instance_name and instance_name in internal_dbs:
                    db_info = internal_dbs[instance_name]
                    if isinstance(db_info, dict) and "dbs" not in db_info:
                        for sub_key, sub_value in db_info.items():
                            for db in sub_value["dbs"]:
                                if db == db_name:
                                    context.log.debug(f"Processing database: host={sub_value['host']}, db_name={db}")
                                    if not launch_countries:
                                        mapping_key = Operations._generate_unique_mapping_key(sub_value, db)
                                        yield DynamicOutput(
                                            value={
                                                "host": sub_value["host"],
                                                "db_name": db.lower().strip(),
                                                "cluster": sub_value.get("cluster"),
                                                "db_type": sub_value["db_type"],
                                                "credential_key": sub_value["credential_key"],
                                                "query": query,
                                                'date_range': date_range,
                                            },
                                            mapping_key=mapping_key,
                                        )
                                    else:
                                        for country in launch_countries:
                                            country_id = Operations._get_country_id(country)
                                            context.log.debug(
                                                f"Processing database: host={sub_value['host']}, db_name={db}")
                                            mapping_key = Operations._generate_unique_mapping_key(sub_value, db,
                                                                                                  country)
                                            yield DynamicOutput(
                                                value={
                                                    "host": sub_value["host"],
                                                    "db_name": db.lower().strip(),
                                                    "db_type": sub_value["db_type"],
                                                    "credential_key": sub_value["credential_key"],
                                                    "country_id": country_id,
                                                    "country_code": country,
                                                    "query": query,
                                                    'date_range': date_range,
                                                },
                                                mapping_key=mapping_key,
                                            )
                    else:
                        for db in db_info["dbs"]:
                            if db == db_name:
                                if not launch_countries:
                                    context.log.debug(f"Processing database: host={db_info['host']}, db_name={db}")
                                    mapping_key = Operations._generate_unique_mapping_key(db_info, db)
                                    yield DynamicOutput(
                                        value={
                                            "host": db_info["host"],
                                            "db_name": db.strip(),
                                            "db_type": db_info["db_type"],
                                            "credential_key": db_info["credential_key"],
                                            "query": query,
                                            'date_range': date_range,
                                        },
                                        mapping_key=mapping_key,
                                    )
                                else:
                                    for country in launch_countries:
                                        country_id = Operations._get_country_id(country)
                                        context.log.debug(f"Processing database: host={db_info['host']}, db_name={db}")
                                        mapping_key = Operations._generate_unique_mapping_key(db_info, db, country)
                                        yield DynamicOutput(
                                            value={
                                                "host": db_info["host"],
                                                "db_name": db.strip(),
                                                "db_type": db_info["db_type"],
                                                "credential_key": db_info["credential_key"],
                                                "country_id": country_id,
                                                "country_code": country,
                                                "query": query,
                                                'date_range': date_range,
                                            },
                                            mapping_key=mapping_key,
                                        )

    @staticmethod
    def _get_country_id(country_code: str) -> int:
        try:
            country_code = country_code.lower().strip('_')
            for country, country_id in map_country_code_to_id.items():
                if country_code == country:
                    return country_id
            raise ValueError(f"Country ID not found for country_code: {country_code}")
        except Exception as e:
            raise ValueError(f"Error in getting country ID for {country_code}:\n{e}")

    @staticmethod
    def _generate_unique_mapping_key(db_info: dict, db_name: str, country: Optional[str] = None) -> str:
        components = [
            db_info["host"],
            db_info.get("cluster", "no_cluster"),
            db_name
        ]
        if country:
            components.append(country)

        raw_key = "_".join(components)
        final_key = re.sub(r'[^A-Za-z0-9_]', '_', raw_key)

        return final_key

    @staticmethod
    def generate_params_dynamically(context, params: dict) -> tuple:
        """
        Generates a tuple of parameters dynamically from a dictionary, filtering keys that start with 'to_sqlcode'.
        """
        try:
            params_tuple = tuple(value for key, value in params.items() if key.startswith("to_sqlcode"))
            context.log.debug(f"Generated params: {params_tuple}")
            return params_tuple
        except Exception as e:
            context.log.error(f"Error generating parameters dynamically:\n{e}")
            raise e

    @staticmethod
    def generate_parquet_file_name(
            partition: int,
            sql_instance_country_query: dict[str, any],
            additional_suffix: Optional[str] = None,
            sequence_number: Optional[int] = None,
    ) -> str:
        """
        Generates a file name for a parquet partition.
        """
        db_name = sql_instance_country_query.get("db_name")
        country_code = sql_instance_country_query.get("country_code", "global")
        current_date = datetime.now().strftime("%Y_%m_%d")

        file_name_parts = [country_code, db_name, "DB"]

        if additional_suffix:
            file_name_parts.append(additional_suffix)
        file_name_parts.append(current_date)

        if sequence_number is not None:
            file_name_parts.append(str(sequence_number))

        rand_code = ''.join(random.choices(string.ascii_letters, k=5))
        file_name_parts.append(rand_code)

        file_name_parts.append(f"part-{partition:05d}")

        return "_".join(file_name_parts) + ".parquet"

    @staticmethod
    def map_postgres_to_pandas_types(pg_type: str) -> str:
        """
        Maps PostgreSQL types to pandas dtypes.

        Args:
            pg_type: PostgreSQL type name

        Returns:
            Corresponding pandas dtype
        """
        type_mapping = {
            # Numeric types
            'smallint': 'Int16',
            'integer': 'Int32',
            'bigint': 'Int64',
            'decimal': 'float64',
            'numeric': 'float64',
            'real': 'float32',
            'double precision': 'float64',

            # Character types
            'character varying': 'string',
            'varchar': 'string',
            'character': 'string',
            'char': 'string',
            'text': 'string',

            # Date/Time types
            'timestamp': 'datetime64[ns]',
            'timestamp with time zone': 'datetime64[ns, UTC]',
            'date': 'datetime64[ns]',
            'time': 'string',

            # Boolean type
            'boolean': 'boolean',

            # Array types
            'ARRAY': 'object',

            # JSON types
            'json': 'object',
            'jsonb': 'object',

            # UUID type
            'uuid': 'string'
        }
        return type_mapping.get(pg_type, 'object')


class DwhOperations:

    @staticmethod
    def save_to_dwh_pandas(context,
                           df,
                           schema,
                           table_name,
                           destination_db: Optional[str] = "dwh",
                           if_exists: Optional[str] = "append"):
        """
        Saves a file to a specified table in the data warehouse.
        """

        def exec_save(context, engine, df, schema, table_name, if_exists):
            try:
                context.log.debug(f"Saving data to >> [{schema}.{table_name}] ...")
                df.to_sql(
                    table_name,
                    con=engine,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    chunksize=100000
                )
                context.log.debug(
                    f"Number of rows: {df.shape[0]}\n"
                    f"Data saved successfully to >> [{schema}.{table_name}]."
                )
            except Exception as e:
                context.log.error(f"Error saving data to >> [{schema}.{table_name}]: {e}")
                raise e

        def save(db_manager_params, context, df, schema, table_name, if_exists):
            db_manager = DatabaseConnectionManager()
            engine = db_manager.get_sqlalchemy_engine(**db_manager_params)
            # connection = engine.raw_connection()
            exec_save(
                context=context,
                schema=schema,
                table_name=table_name,
                engine=engine,
                df=df,
                if_exists=if_exists
            )

        # Save to dwh only
        if destination_db == "dwh":
            save({}, context, df, schema, table_name, if_exists)

        # Save to cloudberry only
        elif destination_db == "cloudberry":
            save({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh',
                "db_type": 'postgresql'
            }, context, df, schema, table_name, if_exists)

        # Save to both dwh and cloudberry
        elif destination_db == "both":
            save({}, context, df, schema, table_name, if_exists)
            save({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh',
                "db_type": 'postgresql'
            }, context, df, schema, table_name, if_exists)

    @staticmethod
    def save_to_dwh_copy_method(context,
                                schema,
                                table_name,
                                df: Optional[pd.DataFrame] = None,
                                file_path: Optional[str] = None,
                                destination_db: Optional[str] = "dwh",
                                cloudberry_db_name: Optional[str] = "an_dwh"):
        """
        Saves data from a DataFrame or a parquet file to a specified table in the data warehouse.

        :param context: The context object for logging.
        :param schema: The schema of the table in the data warehouse.
        :param table_name: The name of the table in the data warehouse.
        :param df: DataFrame to save. (optional)
        :param file_path: Path to the parquet file to save. (optional)
        :param destination_db: The destination database for the data. (optional)

        """
        if df is None and file_path is None:
            raise ValueError("Either df or file_path must be provided")

        def execute_saving(context, schema, table_name, connection,
                           df: Optional[pd.DataFrame] = None,
                           file_path: Optional[str] = None):
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

                    if file_path is not None:
                        context.log.debug(f"Loading data from parquet file: {file_path}")
                        df = pd.read_parquet(file_path, dtype_backend="pyarrow")
                    else:
                        context.log.debug("Loading data from DataFrame...")

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

        def save(db_manager_params, context, schema, table_name, df, file_path):
            db_manager = DatabaseConnectionManager()
            engine = db_manager.get_sqlalchemy_engine(**db_manager_params)
            connection = engine.raw_connection()
            try:
                execute_saving(
                    context=context,
                    schema=schema,
                    table_name=table_name,
                    connection=connection,
                    df=df,
                    file_path=file_path,
                )
            finally:
                connection.close()

        # Save to dwh only
        if destination_db == "dwh":
            save({}, context, schema, table_name, df, file_path)

        # Save to cloudberry only
        elif destination_db == "cloudberry":
            save({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": cloudberry_db_name,
                "db_type": 'postgresql'
            }, context, schema, table_name, df, file_path)

        # Save to both dwh and cloudberry
        elif destination_db == "both":
            save({}, context, schema, table_name, df, file_path)
            save({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": cloudberry_db_name,
                "db_type": 'postgresql'
            }, context, schema, table_name, df, file_path)

    @staticmethod
    def save_to_dwh_upsert(context,
                           schema,
                           table_name,
                           df: Optional[pd.DataFrame] = None,
                           file_path: Optional[str] = None,
                           custom_key: Optional[str] = None,
                           skip_updates: bool = False,
                           exclude_update_columns: Optional[List[str]] = None,
                           destination_db: Optional[str] = "dwh"):
        """
        Upserts data from a DataFrame or a parquet file to a specified table in the data warehouse.

        :param context: The context object for logging.
        :param schema: The schema of the table in the data warehouse.
        :param table_name: The name of the table in the data warehouse.
        :param df: DataFrame to save. (optional)
        :param file_path: Path to the parquet file to save. (optional)
        :param custom_key: Custom key for upsert operations. (optional)
        :param skip_updates: If True, rows with existing keys will be skipped. (optional)
        :param exclude_update_columns: List of column names to exclude from updates during upsert. (optional)
        :param destination_db: Target database ("dwh", "cloudberry", or "both"). (optional)
        """
        if df is None and file_path is None:
            raise ValueError("Either df or file_path must be provided")

        def execute_saving(connection,
                           context,
                           schema,
                           table_name,
                           df,
                           file_path,
                           custom_key,
                           skip_updates,
                           exclude_update_columns
                           ):
            def get_primary_key(schema, table_name) -> str:
                """
                Retrieves the primary key columns of a specified table from the database schema.

                :param schema: The schema of the table in the data warehouse.
                :param table_name: The name of the table in the data warehouse.
                :return: A string representing the primary key columns joined by commas.
                """
                pk_query = f"""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = '{schema}'
                    AND tc.table_name = '{table_name}';
                """
                with connection.cursor() as cur:
                    cur.execute(pk_query)
                    pk_columns = cur.fetchall()
                    if pk_columns:
                        return ', '.join([col[0] for col in pk_columns])
                    else:
                        raise ValueError(f"No primary key found for table {schema}.{table_name}")

            # Query to get column names in their original order from the specified table
            query = f"""
                        SELECT
                            column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position;
                    """

            # Generate a random code to append to the temporary table name
            rand_code = ''.join(random.choices(string.ascii_letters, k=5))
            temp_table_name = f"temp_{table_name}_{rand_code}"

            try:
                with connection.cursor() as cur:
                    # Execute the query to fetch the column names
                    cur.execute(query)
                    columns = cur.fetchall()
                    column_order = [column[0] for column in columns]
                    context.log.debug(f"Target table columns: {column_order}")

                    # Load data from a parquet file if provided
                    if file_path is not None:
                        context.log.debug(f"Loading data from parquet file: {file_path}")
                        df = pd.read_parquet(file_path, dtype_backend="pyarrow")
                    else:
                        context.log.debug("Loading data from DataFrame...")

                    # Reorder columns in the DataFrame to match the original order in the table
                    df = df.reindex(columns=column_order)
                    # Convert DataFrame to CSV format
                    csv_string = df.to_csv(index=False)

                    # Create a temporary table to hold the data
                    context.log.debug(f"Copying data to temporary table {temp_table_name} ...")
                    cur.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE {schema}.{table_name} INCLUDING ALL)")
                    cur.copy_expert(
                        f"COPY {temp_table_name} FROM STDIN WITH (FORMAT CSV, HEADER)",
                        StringIO(csv_string)
                    )

                    # Determine the primary key for the upsert operation
                    context.log.debug(f"Upserting data into {schema}.{table_name} ...")
                    primary_key = custom_key if custom_key else get_primary_key(schema, table_name)
                    context.log.debug(f"Primary key: {primary_key}")

                    # Get list of primary key columns
                    primary_key_columns = set(col.strip() for col in primary_key.split(','))

                    # Initialize exclude columns set with primary key columns
                    exclude_columns = primary_key_columns.copy()

                    # Add user-specified columns to exclude if provided
                    if exclude_update_columns:
                        exclude_columns.update(exclude_update_columns)

                    # Create list of columns to update (excluding primary key and excluded columns)
                    update_columns = [col for col in column_order if col not in exclude_columns]
                    context.log.debug(f"Update columns: {update_columns}")

                    # Perform the upsert operation
                    if skip_updates:
                        # If skip_updates is True, rows with existing keys will be skipped
                        cur.execute(f"""
                            INSERT INTO {schema}.{table_name}
                            SELECT * FROM {temp_table_name}
                            ON CONFLICT ({primary_key})
                            DO NOTHING;
                        """)
                    else:
                        # If skip_updates is False, rows with existing keys will be updated
                        # Create the update statement for the columns that need to be updated
                        update_stmt = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_columns])
                        cur.execute(f"""
                            INSERT INTO {schema}.{table_name}
                            SELECT * FROM {temp_table_name}
                            ON CONFLICT ({primary_key})
                            DO UPDATE SET {update_stmt};
                        """)

                connection.commit()
                context.log.debug(f"Data successfully upserted to >> [{schema}.{table_name}].")
            except Exception as e:
                context.log.error(f"Error upserting data to >> [{schema}.{table_name}]: {e}")
                raise e
            finally:
                # Ensure the temporary table is dropped after the operation
                with connection.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    connection.commit()
                    context.log.debug(f"Temporary table [{temp_table_name}] dropped.")

        def save(db_manager_params,
                 context,
                 schema,
                 table_name,
                 df,
                 file_path,
                 custom_key,
                 skip_updates,
                 exclude_update_columns):
            db_manager = DatabaseConnectionManager()
            engine = db_manager.get_sqlalchemy_engine(**db_manager_params)
            connection = engine.raw_connection()
            try:
                execute_saving(
                    connection=connection,
                    context=context,
                    schema=schema,
                    table_name=table_name,
                    df=df,
                    file_path=file_path,
                    custom_key=custom_key,
                    skip_updates=skip_updates,
                    exclude_update_columns=exclude_update_columns
                )
            finally:
                connection.close()

        # Save to dwh only
        if destination_db == "dwh":
            save(db_manager_params={},
                 context=context,
                 schema=schema,
                 table_name=table_name,
                 df=df,
                 file_path=file_path,
                 custom_key=custom_key,
                 skip_updates=skip_updates,
                 exclude_update_columns=exclude_update_columns)

        # Save to cloudberry only
        elif destination_db == "cloudberry":
            save(db_manager_params={
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh',
                "db_type": 'postgresql'
            },
                context=context,
                schema=schema,
                table_name=table_name,
                df=df,
                file_path=file_path,
                custom_key=custom_key,
                skip_updates=skip_updates,
                exclude_update_columns=exclude_update_columns)

        # Save to both dwh and cloudberry
        elif destination_db == "both":
            save(db_manager_params={},
                 context=context,
                 schema=schema,
                 table_name=table_name,
                 df=df,
                 file_path=file_path,
                 custom_key=custom_key,
                 skip_updates=skip_updates,
                 exclude_update_columns=exclude_update_columns)

            save(db_manager_params={
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh',
                "db_type": 'postgresql'
            },
                context=context,
                schema=schema,
                table_name=table_name,
                df=df,
                file_path=file_path,
                custom_key=custom_key,
                skip_updates=skip_updates,
                exclude_update_columns=exclude_update_columns)

    @staticmethod
    def truncate_dwh_table(context, schema, table_name, destination_db: Optional[str] = "dwh"):
        """
        Truncates a specified table in the data warehouse.
        """

        def execute_truncate(connection, schema, table_name):
            try:
                with connection as conn:
                    with conn.cursor() as cur:
                        context.log.info(f"Starting to truncate table {schema}.{table_name}...")

                        cur.execute(f"TRUNCATE TABLE {schema}.{table_name};")
                        conn.commit()

                context.log.info(f"Table {schema}.{table_name} truncated successfully.")
            except Exception as e:
                context.log.error(f"Error truncating {schema}.{table_name}:\n{e}")
                raise e

        def truncate(
                conn_params,
                schema,
                table_name
        ):
            connection = DatabaseConnectionManager().get_psycopg2_connection(**conn_params)
            execute_truncate(
                connection=connection,
                schema=schema,
                table_name=table_name
            )

        # Delete from dwh only
        if destination_db == "dwh":
            truncate(
                {},
                schema,
                table_name
            )

        # Delete from cloudberry only
        elif destination_db == "cloudberry":
            truncate({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            },
                schema,
                table_name)

        # Delete from both
        elif destination_db == "both":
            truncate(
                {},
                schema,
                table_name
            )
            truncate({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            },
                schema,
                table_name)

    @staticmethod
    def delete_data_from_dwh_table(
            context,
            schema: str,
            table_name: str,
            date_column: Optional[str] = None,
            date_start: Optional[str] = None,
            date_end: Optional[str] = None,
            country_column: Optional[str] = None,
            country: Optional[str] = None,
            force_delete: bool = False,
            skip_check: bool = False,
            destination_db: Optional[str] = "dwh",
            additional_where_clause: Optional[str] = None
    ) -> None:
        """
        Deletes data from a specified DWH table based on country and/or date criteria.
        If no criteria provided, requires force_delete flag to delete all data.

        Args:
            context: Dagster context for logging.
            schema (str): Schema of the table in the data warehouse.
            table_name (str): Name of the table in the data warehouse.
            date_column (Optional[str]): Name of the date column to filter.
            date_start (Optional[str]): Start date for the date range filter.
            date_end (Optional[str]): End date for the date range filter. Defaults to date_start if not provided.
            country_column (Optional[str]): Name of the country column to filter.
            country (Optional[str]): Country ID / Country code / Country name to filter.
            force_delete (bool): Flag to allow deletion without WHERE conditions. Default is False.
            skip_check (bool): Skip checking if data exists before deletion. Default is False.
            destination_db (Optional[str]): The destination database for the data. (optional)
            additional_where_clause (Optional[str]): Additional WHERE clause to filter the data. (optional)
        Raises:
            ValueError: If attempting to delete without criteria and force_delete is False.
        """

        def execute_deletion(
                connection,
                context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
        ):
            try:
                with connection as conn:
                    with conn.cursor() as cursor:
                        # If no criteria provided, check force_delete flag
                        if not any([date_column, country_column]):
                            if not force_delete:
                                error_msg = (
                                    f"Attempting to delete all data from {schema}.{table_name} "
                                    "without WHERE conditions. Set force_delete=True if this is intended."
                                )
                                context.log.error(error_msg)
                                raise ValueError(error_msg)

                            context.log.warning(
                                f"Force delete enabled. Deleting ALL data from {schema}.{table_name}. "
                                "This action cannot be undone!"
                            )
                            delete_sql = f"DELETE FROM {schema}.{table_name};"
                            cursor.execute(delete_sql)
                            conn.commit()
                            context.log.info(f"All data deleted from {schema}.{table_name}")
                            return

                        where_clauses = []
                        params = {}

                        if country_column and country:
                            where_clauses.append(f"{country_column} = %(country)s")
                            params['country'] = country

                        if date_column and date_start:
                            where_clauses.append(f"{date_column} BETWEEN %(date_start)s AND %(date_end)s")
                            params['date_start'] = date_start
                            params['date_end'] = date_end or date_start

                        where_statement = " AND ".join(where_clauses)

                        if additional_where_clause:
                            where_statement += f" AND {additional_where_clause}"

                        # Execute deletion with or without check
                        if skip_check:
                            context.log.info("Skipping data existence check...")
                            delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                            context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                            cursor.execute(delete_sql, params)
                            conn.commit()

                            log_msg = f"Executed deletion from {schema}.{table_name}"
                            if country:
                                log_msg += f"\nCountry: {country}"
                            if date_column and date_start:
                                log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                            context.log.info(log_msg)
                        else:
                            context.log.info("Start checking if data exists in the table...")
                            check_sql = f"SELECT EXISTS(SELECT 1 FROM {schema}.{table_name} WHERE {where_statement});"
                            context.log.debug(f"Check SQL: {check_sql}\nParams: {params}")

                            cursor.execute(check_sql, params)
                            exists = cursor.fetchone()[0]

                            if exists:
                                context.log.info("Initiating deletion of old data from DWH.")

                                delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                                context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                                cursor.execute(delete_sql, params)
                                conn.commit()

                                log_msg = f"Deleted data from {schema}.{table_name}"
                                if country:
                                    log_msg += f"\nCountry: {country}"
                                if date_column and date_start:
                                    log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                                context.log.info(log_msg)
                            else:
                                context.log.info("No data found matching the criteria; no deletion performed.")

            except Exception as e:
                context.log.error(f"Error deleting data from {schema}.{table_name}:\n{e}")
                raise e

        def delete(
                conn_params,
                context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
        ):
            connection = DatabaseConnectionManager().get_psycopg2_connection(**conn_params)
            execute_deletion(
                connection=connection,
                context=context,
                schema=schema,
                table_name=table_name,
                date_column=date_column,
                date_start=date_start,
                date_end=date_end,
                country_column=country_column,
                country=country,
                force_delete=force_delete,
                skip_check=skip_check,
                additional_where_clause=additional_where_clause
            )

        # Delete from dwh only
        if destination_db == "dwh":
            delete(
                {},
                context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
            )

        # Delete from cloudberry only
        elif destination_db == "cloudberry":
            delete({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            }, context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
            )

        # Delete from both
        elif destination_db == "both":
            delete(
                {},
                context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
            )
            delete({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            }, context,
                schema,
                table_name,
                date_column,
                date_start,
                date_end,
                country_column,
                country,
                force_delete,
                skip_check,
                additional_where_clause
            )

    @staticmethod
    def delete_data_from_dwh_table_with_pool(
            context,
            schema: str,
            table_name: str,
            date_column: Optional[str] = None,
            date_start: Optional[str] = None,
            date_end: Optional[str] = None,
            country_column: Optional[str] = None,
            country: Optional[str] = None,
            force_delete: bool = False,
            skip_check: bool = False,
    ) -> None:
        """
        Deletes data from a specified DWH table based on country and/or date criteria.
        If no criteria provided, requires force_delete flag to delete all data.

        Args:
            context: Dagster context for logging.
            schema (str): Schema of the table in the data warehouse.
            table_name (str): Name of the table in the data warehouse.
            date_column (Optional[str]): Name of the date column to filter.
            date_start (Optional[str]): Start date for the date range filter.
            date_end (Optional[str]): End date for the date range filter. Defaults to date_start if not provided.
            country_column (Optional[str]): Name of the country column to filter.
            country (Optional[str]): Country ID / Country code / Country name to filter.
            force_delete (bool): Flag to allow deletion without WHERE conditions. Default is False.
            skip_check (bool): Skip checking if data exists before deletion. Default is False.

        Raises:
            ValueError: If attempting to delete without criteria and force_delete is False.
        """
        db_manager = DatabaseConnectionManager()
        pool = db_manager.get_connection_pool()

        try:
            with pool.connection() as conn:
                with conn.cursor() as cursor:
                    # If no criteria provided, check force_delete flag
                    if not any([date_column, country_column]):
                        if not force_delete:
                            error_msg = (
                                f"Attempting to delete all data from {schema}.{table_name} "
                                "without WHERE conditions. Set force_delete=True if this is intended."
                            )
                            context.log.error(error_msg)
                            raise ValueError(error_msg)

                        context.log.warning(
                            f"Force delete enabled. Deleting ALL data from {schema}.{table_name}. "
                            "This action cannot be undone!"
                        )
                        delete_sql = f"DELETE FROM {schema}.{table_name};"
                        cursor.execute(delete_sql)
                        conn.commit()
                        context.log.info(f"All data deleted from {schema}.{table_name}")
                        return

                    where_clauses = []
                    params = {}

                    if country_column and country:
                        where_clauses.append(f"{country_column} = %(country)s")
                        params['country'] = country

                    if date_column and date_start:
                        where_clauses.append(f"{date_column} BETWEEN %(date_start)s AND %(date_end)s")
                        params['date_start'] = date_start
                        params['date_end'] = date_end or date_start

                    where_statement = " AND ".join(where_clauses)

                    # Execute deletion with or without check
                    if skip_check:
                        context.log.info("Skipping data existence check...")
                        delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                        context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                        cursor.execute(delete_sql, params)
                        conn.commit()

                        log_msg = f"Executed deletion from {schema}.{table_name}"
                        if country:
                            log_msg += f"\nCountry: {country}"
                        if date_column and date_start:
                            log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                        context.log.info(log_msg)
                    else:
                        context.log.info("Start checking if data exists in the table...")
                        check_sql = f"SELECT EXISTS(SELECT 1 FROM {schema}.{table_name} WHERE {where_statement}) as exists_flag;"
                        context.log.debug(f"Check SQL: {check_sql}\nParams: {params}")

                        cursor.execute(check_sql, params)
                        exists = cursor.fetchone()['exists_flag']

                        if exists:
                            context.log.info("Initiating deletion of old data from DWH.")

                            delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                            context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                            cursor.execute(delete_sql, params)
                            conn.commit()

                            log_msg = f"Deleted data from {schema}.{table_name}"
                            if country:
                                log_msg += f"\nCountry: {country}"
                            if date_column and date_start:
                                log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                            context.log.info(log_msg)
                        else:
                            context.log.info("No data found matching the criteria; no deletion performed.")

        except Exception as e:
            context.log.error(f"Error deleting data from {schema}.{table_name}:\n{e}")
            raise e

    # @staticmethod
    # def select_from_dwh(context, sql):
    #     """
    #     Executes a SELECT query on the data warehouse and returns the result as a DataFrame.
    #     """
    #     try:
    #         with DwhConnectionManager.get_psycopg2_connection() as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute(sql)
    #                 df = pd.DataFrame(
    #                     cur.fetchall(), columns=[desc[0] for desc in cur.description]
    #                 )
    #         context.log.info(f"Select query executed successfully: {sql}")
    #         return df
    #     except Exception as e:
    #         context.log.error(f"Error executing select query:\n{e}")
    #         raise e

    @staticmethod
    def execute_on_dwh(
            context,
            query: Optional[str] = None,
            ddl_query: Optional[str] = None,
            params: Optional[tuple] = None,
            fetch_results: bool = False,
            destination_db: Optional[str] = "dwh"
    ):
        """
        Executes the provided SQL statement on the data warehouse.
        Supports execution of an optional DDL query prior to the main query.

        :param context: The context object for logging.
        :param query: The main SQL query to execute.
        :param ddl_query: Optional DDL SQL query to execute before the main query.
        :param params: Optional tuple of parameters to use in the main query.
                    Must be a tuple if provided.
        :param fetch_results: If True, fetch and return the results of the main query.
        :raises TypeError: If params is not a tuple.
        :return: Query results if fetch_results is True, otherwise None.
        """

        def run_exec(engine,
                     context,
                     query,
                     ddl_query,
                     params,
                     fetch_results):
            if params is not None and not isinstance(params, tuple):
                raise TypeError(f"Expected params to be of type tuple, but got {type(params).__name__}")

            def notice_handler(message):
                if message.severity == 'WARNING':
                    context.log.warning(f"{message.severity} - {message.message_primary}")
                    send_formatted_message(
                        message=message.message_primary,
                        severity=message.severity,
                        tag_dwh=False
                    )
                elif message.severity == 'EXCEPTION':
                    context.log.error(f"{message.severity} - {message.message_primary}")
                    send_formatted_message(
                        message=message.message_primary,
                        severity=message.severity,
                        tag_dwh=True
                    )
                    raise Failure
                else:
                    context.log.info(f"{message.severity} - {message.message_primary}")

            results = None

            try:
                with engine as conn:
                    with conn.cursor() as cursor:
                        conn.add_notice_handler(notice_handler)
                        # Execute DDL query if provided
                        if ddl_query:
                            context.log.debug(f"Executing DDL query: {ddl_query}")
                            cursor.execute(ddl_query)
                            conn.commit()
                            context.log.debug("DDL query executed successfully")

                        # Execute main query if provided
                        if query:
                            context.log.debug(f"Executing main query: {query}.\nParameters: {params}")
                            cursor.execute(query, params)
                            context.log.debug("Main query executed successfully.")

                        if fetch_results:
                            results = cursor.fetchall()
                            context.log.debug(f"Query results fetched. Results: {results[:5]}")
                        else:
                            conn.commit()

                return results

            except Exception as e:
                context.log.error(f"Error executing query on DWH:\n{e}")
                raise e

        def execute(db_manager_params, context, query, ddl_query, params, fetch_results):
            db_manager = DatabaseConnectionManager()
            engine = db_manager.get_psycopg_connection(**db_manager_params)
            return run_exec(engine, context, query, ddl_query, params, fetch_results)

        if destination_db == "dwh":
            return execute(
                {},
                context,
                query,
                ddl_query,
                params,
                fetch_results
            )

        elif destination_db == "cloudberry":
            return execute({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            }, context,
                query,
                ddl_query,
                params,
                fetch_results
            )

        elif destination_db == "both":
            results_dwh = execute(
                {},
                context,
                query,
                ddl_query,
                params,
                fetch_results
            )
            results_cloudberry = execute({
                "credential_key": "cloudberry",
                "host": 'nl-cloudberrydb-coordinator-1.jooble.com',
                "database": 'an_dwh'
            }, context,
                query,
                ddl_query,
                params,
                fetch_results
            )

            return results_dwh

    @staticmethod
    def maintain_table_retention(
            context,
            schema: str,
            table_name: str,
            date_column: str,
            date_type: str = 'datediff',
            retention_days: int = 365,
            archive_schema: Optional[str] = None,
            archive_table: Optional[str] = None,
            archive_enabled: bool = True) -> None:
        """
        Maintains the given table within a rolling window of retention_days days.
        Rows older than the retention period are either moved to the archive table or deleted directly.

        Args:
            context: The Dagster context object for logging.
            schema (str): The schema of the target table.
            table_name (str): The target table name.
            date_column (str): The name of the date column used for retention filtering.
            date_type (str): The type of the date column - 'datediff', 'date', 'datetime'. Default is 'datediff'.
            retention_days (int): Number of days to retain data in the target table. Default is 365.
            archive_schema (Optional[str]): The schema of the archive table. Default is None.
            archive_table (Optional[str]): The archive table name where old records are stored. Default is None.
            archive_enabled (bool): If True, old records are moved to archive. If False, they are deleted directly. Default is True.
        """
        from datetime import datetime, timedelta
        # Calculate the cutoff date
        cutoff_datetime_str = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d 00:00:00.000')
        cutoff_date_str = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
        cutoff_datediff = get_datediff(cutoff_date_str)

        match date_type:
            case 'datediff':
                cutoff_date = cutoff_datediff
            case 'date':
                cutoff_date = cutoff_date_str
            case 'datetime':
                cutoff_date = cutoff_datetime_str
            case _:
                raise ValueError(f"Invalid date type: {date_type}")

        check_sql = f"""
            SELECT EXISTS(
                SELECT 1
                FROM {schema}.{table_name}
                WHERE {date_column} < %(cutoff_date)s
            ) as exists_flag;
        """

        # SQL to insert data older than cutoff_date into the archive table (only if archiving is enabled)
        insert_sql = f"""
            INSERT INTO {archive_schema}.{archive_table}
            SELECT *
            FROM {schema}.{table_name}
            WHERE {date_column} < %(cutoff_date)s;
        """
        # SQL to delete data older than cutoff_date from the target table
        delete_sql = f"""
            DELETE FROM {schema}.{table_name}
            WHERE {date_column} < %(cutoff_date)s;
        """

        db_manager = DatabaseConnectionManager()
        db_params = {
            "credential_key": "cloudberry",
            "host": "nl-cloudberrydb-coordinator-1.jooble.com",
            "database": "an_dwh"
        }

        try:
            connection = db_manager.get_psycopg_connection(**db_params)

            with connection as conn:
                with conn.cursor() as cur:
                    params = {'cutoff_date': cutoff_date}

                    context.log.info("Start checking if data exists in the table...")
                    context.log.debug(f"Check SQL: {check_sql}\nCutoff date: {cutoff_date}")

                    cur.execute(check_sql, params)
                    exists = cur.fetchone()['exists_flag']

                    if exists:
                        if archive_enabled:
                            # Insert old records into the archive table
                            context.log.info("Inserting old records into the archive table...")
                            cur.execute(insert_sql, params)
                            context.log.info("Successfully archived old records.")

                        # Delete old records from the target table
                        context.log.info("Deleting old records from the target table...")
                        cur.execute(delete_sql, params)
                        conn.commit()

                        action = "archived and deleted" if archive_enabled else "deleted"
                        context.log.info(f"Successfully {action} records older than {cutoff_date_str} from {schema}.{table_name}.")
                    else:
                        context.log.info(f"No data found in the table - {schema}.{table_name} older than {cutoff_date_str}.")
        except Exception as e:
            context.log.error(f"Error during table retention maintenance: {e}")
            raise e


class DbOperations:

    @staticmethod
    def execute_query_and_save_to_parquet(
            context,
            path_to_data,
            sql_instance_country_query: dict[str, any],
            country_column: Optional[str] = None,
    ) -> list[str]:
        """
        Executes a SQL query on a database and saves the results to Parquet files.

        :param context: The dagster context object for logging.
        :param path_to_data: The path where Parquet files will be saved.
        :param sql_instance_country_query: Dictionary containing SQL instance details and query information.
        :param country_column: Optional column name to add to the DataFrame with the country_id. Defaults to 'country_id'.
        :return: List of file paths to the saved Parquet files.
        """

        db_manager = DatabaseConnectionManager()
        connection = db_manager.get_sqlalchemy_engine(
            credential_key=sql_instance_country_query["credential_key"],
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
            db_type=sql_instance_country_query.get("db_type"),
        ).connect()

        sql = sql_instance_country_query["formatted_query"] if "formatted_query" in sql_instance_country_query \
            else text(sql_instance_country_query["query"])
        cluster = sql_instance_country_query.get("cluster")
        params = {key: val for key, val in sql_instance_country_query.items() if key.startswith("to_sql")}

        try:
            with connection as conn:
                context.log.debug("Executing SQL query...")
                context.log.debug(f"Query: {sql}; Params: {params}")

                chunks = pd.read_sql_query(
                    sql=sql,
                    con=conn,
                    params=params or None,
                    chunksize=500000,
                    dtype_backend="pyarrow",
                )
                context.log.debug("SQL query executed.")

                file_paths = []
                for i, chunk in enumerate(chunks, start=1):

                    if chunk.empty:
                        context.log.info("No data returned from query; no files were saved.")
                        return None

                    try:
                        context.log.debug(f"Processing chunk [{i}]... Rows: {chunk.shape[0]}")

                        if country_column is not None:
                            country_field_name = country_column or "country_id"
                            chunk[country_field_name] = sql_instance_country_query["country_id"]

                        if (cluster is not None) and (cluster in ('us', 'nl')):
                            chunk['cluster'] = 'nl' if cluster == 'nl' else 'us'

                        file_path = Operations.save_dataframe_as_parquet(
                            context,
                            chunk,
                            path_to_data,
                            sql_instance_country_query,
                            sequence_number=i,
                        )
                        file_paths.append(file_path)
                    except Exception as e:
                        context.log.error(f"Error processing chunk [{i}]: {e}")
                        raise e

                return file_paths

        except Exception as e:
            context.log.error(f"Query execution error: {e}")
            raise e
        finally:
            if 'connection' in locals() and not connection.closed:
                context.log.debug("Closing database connection.")
                connection.close()

    @staticmethod
    def execute_query_and_return_chunks(
            context,
            sql_instance_country_query: dict[str, any],
            country_column: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Executes a SQL query on a database and returns the results as a generator of DataFrames.

        :param context: The dagster context object for logging.
        :param sql_instance_country_query: Dictionary containing SQL instance details and query information.
        :param country_column: Optional column name to add to the DataFrame with the country_id. Defaults to 'country_id'.
        :return: Generator yielding DataFrames with query results.
        """
        db_manager = DatabaseConnectionManager()
        connection = db_manager.get_sqlalchemy_engine(
            credential_key=sql_instance_country_query["credential_key"],
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
            db_type=sql_instance_country_query.get("db_type")
        ).connect()

        sql = sql_instance_country_query["formatted_query"] if "formatted_query" in sql_instance_country_query \
            else text(sql_instance_country_query["query"])
        cluster = sql_instance_country_query.get("cluster")
        params = {key: val for key, val in sql_instance_country_query.items() if key.startswith("to_sql")}

        try:
            with connection as conn:
                context.log.debug("Executing SQL query...")
                context.log.debug(f"Query: {sql}; Params: {params}")

                chunks = pd.read_sql_query(
                    sql=sql,
                    con=conn,
                    params=params or None,
                    chunksize=500000,
                    dtype_backend="pyarrow"
                )
                context.log.debug("SQL query executed.")

                for i, chunk in enumerate(chunks, start=1):

                    if chunk.empty:
                        context.log.info("No data returned from query; no chunks were retrieved.")
                        break

                    context.log.debug(f"Processing chunk [{i}]... Rows: {chunk.shape[0]}")

                    if country_column is not None:
                        country_field_name = country_column or "country_id"
                        chunk[country_field_name] = sql_instance_country_query["country_id"]

                    if (cluster is not None) and (cluster in ('us', 'nl')):
                            chunk['cluster'] = 'nl' if cluster == 'nl' else 'us'

                    yield chunk

        except Exception as e:
            context.log.error(f"Query execution error: {e}")
            raise e
        finally:
            if 'connection' in locals() and not connection.closed:
                context.log.debug("Closing database connection.")
                connection.close()

    # @staticmethod
    # def create_procedure(context, sql_instance_country_query: dict):
    #     db_manager = DatabaseConnectionManager()
    #     connection = db_manager.get_psycopg2_connection(
    #         credential_key=sql_instance_country_query["credential_key"],
    #         host=sql_instance_country_query.get("host"),
    #         database=sql_instance_country_query.get("db_name"),
    #     )
    #     try:
    #         ddl_query = sql_instance_country_query['ddl_query']

    #         with connection as conn:
    #             cursor = conn.cursor()

    #             # Execute DDL query to create the procedure
    #             cursor.execute(ddl_query)
    #             context.log.info(f'Procedure created: {sql_instance_country_query["db_name"]}')
    #             conn.commit()

    #     except Exception as e:
    #         context.log.error(f"Error creating procedure for: {sql_instance_country_query['db_name']}\n{e}")
    #         raise e

    # @staticmethod
    # def call_procedure(context, sql_instance_country_query: dict):

    #     db_manager = DatabaseConnectionManager()
    #     connection = db_manager.get_psycopg2_connection(
    #         credential_key=sql_instance_country_query["credential_key"],
    #         host=sql_instance_country_query.get("host"),
    #         database=sql_instance_country_query.get("db_name"),
    #     )
    #     try:
    #         params = Operations.generate_params_dynamically(context, sql_instance_country_query)
    #         query = sql_instance_country_query['procedure_call']

    #         with connection as conn:
    #             cursor = conn.cursor()
    #             context.log.debug(f"Executing query: {query} with params: {params}")

    #             # Execute the stored procedure
    #             cursor.execute(query, params)
    #             conn.commit()

    #             context.log.info(
    #                 f"Stored procedure executed successfully for {sql_instance_country_query['db_name']}, Params: {params}")

    #     except Exception as e:
    #         context.log.error(f"Error executing procedure for: {sql_instance_country_query['db_name']}\nException: {e}")
    #         raise e

    @staticmethod
    def save_to_clickhouse(context, database, df, table_name):
        try:
            client = DatabaseConnectionManager().get_clickhouse_driver_client(database)
            client.insert_dataframe(query=f"INSERT INTO {table_name} VALUES", dataframe=df)
            context.log.debug(f'Data inserted to Clickhouse for: {database}, {df.shape[0]} rows')

        except Exception as e:
            Failure(f"Error while inserting data to Clickhouse: {database}\n {e}")
            raise e

    @staticmethod
    def create_procedure(context, sql_instance_country_query: dict):
        """
        Creates a stored procedure using connection pool.

        Args:
            context: Dagster execution context
            sql_instance_country_query: Dictionary containing connection details and query
        """
        db_manager = DatabaseConnectionManager()
        pool = db_manager.get_connection_pool(
            credential_key=sql_instance_country_query["credential_key"],
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
        )

        try:
            ddl_query = sql_instance_country_query['ddl_query']

            with pool.connection() as conn:
                with conn.cursor() as cursor:
                    # Execute DDL query to create the procedure
                    cursor.execute(ddl_query)
                    context.log.info(f'Procedure created: {sql_instance_country_query["db_name"]}')

        except Exception as e:
            context.log.error(f"Error creating procedure for: {sql_instance_country_query['db_name']}\n{e}")
            raise e

    @staticmethod
    def call_procedure(context, sql_instance_country_query: dict):
        """
        Calls a stored procedure using connection pool.

        Args:
            context: Dagster execution context
            sql_instance_country_query: Dictionary containing connection details and query
        """
        db_manager = DatabaseConnectionManager()
        pool = db_manager.get_connection_pool(
            credential_key=sql_instance_country_query["credential_key"],
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
        )

        conn = None  # Global variable for access from signal handlers
        query_exception = None
        query_completed = False

        def run_query():
            nonlocal conn, query_exception, query_completed  # Use nonlocal to modify the conn variable from the outer scope
            try:
                params = Operations.generate_params_dynamically(context, sql_instance_country_query)
                query = sql_instance_country_query['procedure_call']

                with pool.connection() as connection:
                    connection.autocommit = True
                    conn = connection  # Save a reference to the connection for access from signal handlers
                    with conn.cursor() as cursor:
                        context.log.debug(f"Executing query: {query} with params: {params}")

                        # Execute the stored procedure
                        cursor.execute(query, params)

                        context.log.info(
                            f"Stored procedure executed successfully for {sql_instance_country_query['db_name']}, "
                            f"Params: {params}"
                        )
                        query_completed = True

            except Exception as e:
                query_exception = e
            finally:
                if conn:
                    conn.close()
                    conn = None

        def signal_handler(sig, frame):
            context.log.warning(f"Signal received: {sig}. Cancelling database operation.")
            if conn:
                try:
                    conn.cancel()  # Send a request to cancel the operation
                    context.log.info("Database operation cancelled.")
                except Exception as e:
                    context.log.error(f"Error cancelling operation: {e}")
                finally:
                    conn.close()
                    context.log.info("Database connection closed.")
            else:
                context.log.warning("No active connection to cancel.")
            sys.exit(0)

        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Run the query in a separate thread
        query_thread = threading.Thread(target=run_query)
        query_thread.start()

        # Wait for the thread to finish
        query_thread.join()

        # After thread completion, check for exceptions
        if query_exception:
            context.log.error(
                f"Error executing procedure for: {sql_instance_country_query['db_name']}\n"
                f"Exception: {query_exception}"
            )
            raise query_exception

        if not query_completed:
            raise Exception("Query execution did not complete successfully")

    @staticmethod
    def save_to_pg_copy_method(
        context,
        sql_instance_country_query: dict[str, any],
        schema,
        table_name,
        df: Optional[pd.DataFrame] = None,
        file_path: Optional[str] = None):
        """
        Saves data from a DataFrame or a parquet file to a specified table in the data warehouse.

        :param context: The context object for logging.
        :param schema: The schema of the table in the data warehouse.
        :param table_name: The name of the table in the data warehouse.
        :param df: DataFrame to save. (optional)
        :param file_path: Path to the parquet file to save. (optional)
        """
        if df is None and file_path is None:
            raise ValueError("Either df or file_path must be provided")

        db_manager = DatabaseConnectionManager()
        engine = db_manager.get_sqlalchemy_engine(
            credential_key=sql_instance_country_query.get("credential_key"),
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
            db_type=sql_instance_country_query.get("db_type")
        )
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

                if file_path is not None:
                    context.log.debug(f"Loading data from parquet file: {file_path}")
                    df = pd.read_parquet(file_path, dtype_backend="pyarrow")
                else:
                    context.log.debug("Loading data from DataFrame...")

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

    @staticmethod
    def delete_data_from_table(
            context,
            sql_instance_country_query: dict[str, any],
            schema: str,
            table_name: str,
            date_column: Optional[str] = None,
            date_start: Optional[str] = None,
            date_end: Optional[str] = None,
            country_column: Optional[str] = None,
            country: Optional[str] = None,
            force_delete: bool = False,
            skip_check: bool = False,
    ) -> None:
        """
        Deletes data from a specified table based on country and/or date criteria.
        If no criteria provided, requires force_delete flag to delete all data.

        Args:
            context: Dagster context for logging.
            sql_instance_country_query (dict): Dictionary containing connection details and query
            schema (str): Schema of the table in the data warehouse.
            table_name (str): Name of the table in the data warehouse.
            date_column (Optional[str]): Name of the date column to filter.
            date_start (Optional[str]): Start date for the date range filter.
            date_end (Optional[str]): End date for the date range filter. Defaults to date_start if not provided.
            country_column (Optional[str]): Name of the country column to filter.
            country (Optional[str]): Country ID / Country code / Country name to filter.
            force_delete (bool): Flag to allow deletion without WHERE conditions. Default is False.
            skip_check (bool): Skip checking if data exists before deletion. Default is False.

        Raises:
            ValueError: If attempting to delete without criteria and force_delete is False.
        """
        connection = DatabaseConnectionManager().get_psycopg_connection(
            credential_key=sql_instance_country_query.get("credential_key"),
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
        )

        try:
            with connection as conn:
                with conn.cursor() as cursor:
                    # If no criteria provided, check force_delete flag
                    if not any([date_column, country_column]):
                        if not force_delete:
                            error_msg = (
                                f"Attempting to delete all data from {schema}.{table_name} "
                                "without WHERE conditions. Set force_delete=True if this is intended."
                            )
                            context.log.error(error_msg)
                            raise ValueError(error_msg)

                        context.log.warning(
                            f"Force delete enabled. Deleting ALL data from {schema}.{table_name}. "
                            "This action cannot be undone!"
                        )
                        delete_sql = f"DELETE FROM {schema}.{table_name};"
                        cursor.execute(delete_sql)
                        conn.commit()
                        context.log.info(f"All data deleted from {schema}.{table_name}")
                        return

                    where_clauses = []
                    params = {}

                    if country_column and country:
                        where_clauses.append(f"{country_column} = %(country)s")
                        params['country'] = country

                    if date_column and date_start:
                        where_clauses.append(f"{date_column} BETWEEN %(date_start)s AND %(date_end)s")
                        params['date_start'] = date_start
                        params['date_end'] = date_end or date_start

                    where_statement = " AND ".join(where_clauses)

                    # Execute deletion with or without check
                    if skip_check:
                        context.log.info("Skipping data existence check...")
                        delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                        context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                        cursor.execute(delete_sql, params)
                        conn.commit()

                        log_msg = f"Executed deletion from {schema}.{table_name}"
                        if country:
                            log_msg += f"\nCountry: {country}"
                        if date_column and date_start:
                            log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                        context.log.info(log_msg)
                    else:
                        context.log.info("Start checking if data exists in the table...")
                        check_sql = f"SELECT EXISTS(SELECT 1 FROM {schema}.{table_name} WHERE {where_statement}) AS exists_flag;"
                        context.log.debug(f"Check SQL: {check_sql}\nParams: {params}")

                        cursor.execute(check_sql, params)
                        exists = cursor.fetchone()['exists_flag']

                        if exists:
                            context.log.info("Initiating deletion of old data from DWH.")

                            delete_sql = f"DELETE FROM {schema}.{table_name} WHERE {where_statement};"
                            context.log.debug(f"Deletion SQL: {delete_sql}\nParams: {params}")

                            cursor.execute(delete_sql, params)
                            conn.commit()

                            log_msg = f"Deleted data from {schema}.{table_name}"
                            if country:
                                log_msg += f"\nCountry: {country}"
                            if date_column and date_start:
                                log_msg += f"\nDate range: {params['date_start']} - {params['date_end']}"
                            context.log.info(log_msg)
                        else:
                            context.log.info("No data found matching the criteria; no deletion performed.")

        except Exception as e:
            context.log.error(f"Error deleting data from {schema}.{table_name}:\n{e}")
            raise e

    @staticmethod
    def execute_query_and_return_chunks_with_pool(
            context,
            sql_instance_country_query: dict[str, any],
            country_column: Optional[str] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Executes a SQL query and returns results as a generator of DataFrames using connection pool.
        Test version of execute_query_and_return_chunks method.

        Args:
            context: Dagster execution context
            sql_instance_country_query: Dictionary containing connection details and query
            country_column: Optional column name for country ID

        Yields:
            Generator yielding DataFrames with query results
        """
        db_manager = DatabaseConnectionManager()
        pool = db_manager.get_connection_pool(
            credential_key=sql_instance_country_query["credential_key"],
            host=sql_instance_country_query.get("host"),
            database=sql_instance_country_query.get("db_name"),
        )

        sql = (sql_instance_country_query["formatted_query"]
               if "formatted_query" in sql_instance_country_query
               else sql_instance_country_query["query"])

        params = tuple(val for key, val in sql_instance_country_query.items()
                       if key.startswith("to_sql"))

        try:
            with pool.connection() as conn:
                with conn.cursor() as cursor:
                    context.log.debug("Executing SQL query...")
                    context.log.debug(f"Query: {sql}; Params: {params}")

                    cursor.execute(sql, params or None, prepare=True)

                    while True:
                        # Fetch chunk of records
                        records = cursor.fetchmany(500000)
                        if not records:
                            break

                        df = pd.DataFrame.from_records(records)

                        if country_column is not None:
                            country_field_name = country_column or "country_id"
                            df[country_field_name] = sql_instance_country_query["country_id"]

                        context.log.debug(f"Processing chunk... Rows: {df.shape[0]}")
                        yield df

                # chunks = pd.read_sql_query(
                #     sql=sql,
                #     con=conn,
                #     params=params or None,
                #     chunksize=500000,
                #     dtype_backend="pyarrow"
                # )
                # context.log.debug("SQL query executed.")

                # for i, chunk in enumerate(chunks, start=1):

                #     if chunk.empty:
                #         context.log.info("No data returned from query; no chunks were retrieved.")
                #         break

                #     context.log.debug(f"Processing chunk [{i}]... Rows: {chunk.shape[0]}")

                #     if country_column is not None:
                #         country_field_name = country_column or "country_id"
                #         chunk[country_field_name] = sql_instance_country_query["country_id"]

                #     yield chunk

        except Exception as e:
            context.log.error(f"Query execution error: {e}")
            raise e

    @staticmethod
    def save_to_dwh_copy_method_with_pool(
            context,
            schema: str,
            table_name: str,
            df: pd.DataFrame
    ) -> None:
        """
        Saves DataFrame to DWH using COPY protocol and connection pool.
        Test version of save_to_dwh_copy_method method.

        Args:
            context: Dagster execution context
            schema: Target schema name
            table_name: Target table name
            df: DataFrame to save
        """
        if df.empty:
            context.log.warning("DataFrame is empty, skipping upload")
            return

        db_manager = DatabaseConnectionManager()
        pool = db_manager.get_connection_pool()

        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    # Get column information from the target table
                    query = f"""
                        SELECT
                            column_name,
                            data_type,
                            udt_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position;
                    """
                    cur.execute(query)
                    columns = cur.fetchall()

                    # Log column information
                    for col in columns:
                        context.log.debug(
                            f"Column: {col['column_name']}, "
                            f"Type: {col['data_type']}, "
                            f"UDT: {col['udt_name']}"
                        )

                    column_order = [col['column_name'] for col in columns]
                    pg_types = [col['udt_name'] for col in columns]

                    # Convert DataFrame types
                    dtype_map = {
                        col['column_name']: Operations.map_postgres_to_pandas_types(col['data_type'])
                        for col in columns
                    }

                    context.log.debug(f"Converting DataFrame types: {dtype_map}")
                    df = df.astype(dtype_map)

                    # Log DataFrame info
                    context.log.debug("DataFrame info after type conversion:")
                    for col in df.columns:
                        context.log.debug(f"Column: {col}, Type: {df[col].dtype}, Sample: {df[col].iloc[0]}")

                    # Reorder DataFrame columns
                    df = df.reindex(columns=column_order)

                    # Construct COPY command
                    copy_command = f"""
                        COPY {schema}.{table_name} FROM STDIN WITH (FORMAT CSV)
                    """

                    context.log.debug(f"Starting COPY operation for {schema}.{table_name}")
                    context.log.debug(f"Total rows to copy: {df.shape[0]}")

                    # Convert DataFrame to CSV string
                    csv_string = df.to_csv(
                        index=False,
                        header=False
                    )

                    # Execute COPY operation
                    with cur.copy(copy_command) as copy:
                        copy.set_types(pg_types)
                        copy.write(csv_string)

                context.log.info(
                    f"Successfully copied {df.shape[0]} rows to {schema}.{table_name}"
                )

        except Exception as e:
            context.log.error(
                f"Error copying data to {schema}.{table_name}\n"
                f"Error: {str(e)}"
            )
            raise


class TrinoOperations:
    """
    Class for executing SQL code on Trino.
    """

    @staticmethod
    def exec_on_trino(context, sql_code_to_execute, catalog: str):
        """
        Executes SQL code on a Trino server with graceful interrupt handling.

        This method establishes a connection to a Trino server, executes the provided SQL code,
        and handles potential interruptions (SIGINT, SIGTERM) by properly canceling queries
        and closing connections.

        Args:
            context: Dagster context object for logging
            sql_code_to_execute: SQL query or statement to execute on Trino
            catalog: Trino catalog name to connect to (e.g., 'dwh')

        Raises:
            Failure: If the SQL execution fails for any reason

        Note:
            This method registers signal handlers during execution and restores
            the original handlers before returning.
        """
        conn = None
        cursor = None
        # Establish a Trino connection
        conn = DatabaseConnectionManager.get_trino_connection(catalog=catalog)

        # Create a flag to track if the operation was interrupted
        interrupted = False

        def signal_handler(sig, frame):
            # Handle interrupt: cancel running query and close resources
            nonlocal interrupted
            context.log.warning(f"Received interrupt signal {sig}. Cleaning up...")
            interrupted = True

            if cursor:
                try:
                    context.log.info("Attempting to cancel the running Trino query...")
                    cursor.cancel()
                    context.log.info("Successfully sent cancellation request to Trino server")
                except Exception as e:
                    context.log.error(f"Failed to cancel Trino query: {e}")

                try:
                    cursor.close()
                except Exception as e:
                    context.log.error(f"Error closing cursor during interrupt: {e}")

            if conn:
                try:
                    context.log.info("Closing Trino connection...")
                    conn.close()
                    context.log.info("Connection closed successfully")
                except Exception as e:
                    context.log.error(f"Error closing connection during interrupt: {e}")

            context.log.info(f"Cleanup completed. Raising KeyboardInterrupt for graceful shutdown.")
            raise KeyboardInterrupt

        # Register signal handlers for common interrupt signals
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            cursor = conn.cursor()
            cursor.execute(sql_code_to_execute)
            context.log.info("Successfully executed SQL code on Trino")
        except KeyboardInterrupt:
            # Propagate interruption
            raise
        except Exception as e:
            # Restore original handlers before raising
            raise Failure(f"Failed to execute SQL code on Trino:\n{e}") from e
        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)
            # Ensure cursor is closed
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    context.log.error(f"Error closing cursor: {e}")
            # Close connection if not closed by interrupt
            if conn and not interrupted:
                try:
                    conn.close()
                except Exception as e:
                    context.log.error(f"Error closing connection: {e}")

    @staticmethod
    def fetch_data_from_trino(
            context,
            sql_code_to_execute: str,
            catalog: str,
            params: Optional[Any] = None,
            chunk_size: int = 400000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Executes a SQL query on Trino and returns the results as a generator of pandas DataFrames.

        Args:
            context: Dagster context for logging
            sql_code_to_execute: SQL query to execute
            catalog: Trino catalog name
            params: Query parameters - must be a list, tuple, or dict for Trino's cursor.execute
                   If a dict is provided, it will be converted to a tuple
            chunk_size: Number of rows to fetch at once

        Yields:
            Generator of pandas DataFrames containing query results
        """
        conn = None
        cursor = None
        try:
            conn = DatabaseConnectionManager.get_trino_connection(catalog=catalog)
            cursor = conn.cursor()

            # Execute query with optional params
            if params is not None:
                # Ensure params is in the correct format (list or tuple)
                if isinstance(params, dict):
                    context.log.warning("Converting dict params to tuple for Trino compatibility")
                    # Convert dict to tuple of values in some deterministic order
                    params_tuple = tuple(params.values())
                    cursor.execute(sql_code_to_execute, params_tuple)
                elif isinstance(params, (list, tuple)):
                    cursor.execute(sql_code_to_execute, params)
                else:
                    raise ValueError(f"params must be a list, tuple, or dict, got {type(params)}")
            else:
                cursor.execute(sql_code_to_execute)

            total_rows = 0
            while True:
                try:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    chunk_df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
                    if not chunk_df.empty:
                        total_rows += len(chunk_df)
                        context.log.info(f"Fetched chunk of {len(chunk_df)} rows, total so far: {total_rows}")
                        yield chunk_df
                except Exception:
                    context.log.error("Error fetching chunk", exc_info=True)
                    raise
        except KeyboardInterrupt:
            # Propagate user interruption
            raise
        except Exception as e:
            context.log.error("Failed to fetch data from Trino", exc_info=True)
            raise Failure("Failed to fetch data from Trino") from e
        finally:
            if cursor:
                try:
                    cursor.cancel()
                except Exception as err:
                    context.log.error(f"Failed to cancel Trino query: {err}")
                try:
                    cursor.close()
                except Exception as err:
                    context.log.error(f"Error closing cursor: {err}")
            if conn:
                try:
                    conn.close()
                except Exception as err:
                    context.log.error(f"Error closing Trino connection: {err}")

    @staticmethod
    def replace_database_references(sql_query: str, current_catalog: str) -> str:
        """
        Replace database references in SQL query with the current catalog's database.

        Args:
            sql_query (str): Original SQL query
            current_catalog (str): Current Trino catalog (e.g., 'prod_de', 'prod_fr')

        Returns:
            str: SQL query with updated database references
        """
        country_code = TrinoOperations.get_country_code_from_catalog(current_catalog)
        if not country_code:
            return sql_query

        # Pattern to match database references like rpl_de.an.table_name
        # This regex captures references like: rpl_XX.schema.table
        pattern = r'(rpl_[a-z]{2})\.([a-z_]+)\.([a-z_]+)'

        # Get the correct database prefix based on current catalog
        db_prefix = f"rpl_{country_code}"

        # Replace all database references
        def replacer(match):
            schema = match.group(2)
            table = match.group(3)
            return f"{db_prefix}.{schema}.{table}"

        modified_sql = re.sub(pattern, replacer, sql_query)
        return modified_sql

    @staticmethod
    def get_country_code_from_catalog(catalog: str) -> Optional[str]:
        """
        Extract country code from Trino catalog name (e.g., prod_de -> de)

        Args:
            catalog (str): Trino catalog name (e.g., 'prod_de')

        Returns:
            Optional[str]: Country code if found, None otherwise
        """
        from utility_hub.data_collections import trino_catalog_map

        # Try to find country code in the mapping
        for country_code, cat in trino_catalog_map.items():
            if cat == catalog:
                return country_code

        # If not found in the mapping, try to extract from the name
        if catalog.startswith('prod_'):
            country_code = catalog[5:]  # Remove 'prod_' prefix
            return country_code

        return None

    @staticmethod
    def get_country_cluster(country_code: str) -> Optional[str]:
        """
        Determine which replica cluster a country belongs to

        Args:
            country_code (str): Country code (e.g., 'de', 'us')

        Returns:
            Optional[str]: Cluster name ('nl_cluster' or 'us_cluster') if found, None otherwise
        """
        if country_code in repstat_dbs["nl_cluster"]["dbs"]:
            return "nl_cluster"
        elif country_code in repstat_dbs["us_cluster"]["dbs"]:
            return "us_cluster"
        return None

    @staticmethod
    def prepare_catalog_info(context, catalog: str) -> Optional[dict]:
        """
        Prepare catalog info dictionary with all necessary details for processing

        Args:
            context: Dagster context for logging
            catalog (str): Trino catalog name

        Returns:
            Optional[dict]: Dictionary with catalog info if successful, None otherwise
        """
        # Get country code from catalog
        country_code = TrinoOperations.get_country_code_from_catalog(catalog)
        if not country_code:
            context.log.warning(f"Could not determine country code for catalog: {catalog}")
            return None

        # Determine which replica cluster the country belongs to
        cluster_name = TrinoOperations.get_country_cluster(country_code)
        if not cluster_name:
            context.log.warning(f"Could not determine replica cluster for country: {country_code}")
            return None

        cluster_host = repstat_dbs[cluster_name]["host"]

        # Get date range from globals
        date_range = None
        if "reload_date_start" in context.resources.globals and "reload_date_end" in context.resources.globals:
            date_range = pd.date_range(
                pd.to_datetime(context.resources.globals["reload_date_start"]),
                pd.to_datetime(context.resources.globals["reload_date_end"])
            )

        # Create a simplified catalog info dict with just what we need
        catalog_info = {
            'trino_catalog': catalog,
            'country_code': country_code,
            'cluster_name': cluster_name,
            'host': cluster_host,
            'date_range': date_range
        }

        return catalog_info

    @staticmethod
    def save_to_replica_copy_method(
        context,
        sql_instance_country_query: dict[str, any],
        schema: str,
        table_name: str,
        df: Optional[pd.DataFrame] = None,
        file_path: Optional[str] = None
    ):
        """
        Saves data from a DataFrame or a parquet file to a specified table in the replica database.

        :param context: The context object for logging.
        :param sql_instance_country_query: Dictionary containing SQL instance details and query information.
        :param schema: The schema of the table in the data warehouse.
        :param table_name: The name of the table in the data warehouse.
        :param df: DataFrame to save. (optional)
        :param file_path: Path to the parquet file to save. (optional)
        """
        if df is None and file_path is None:
            raise ValueError("Either df or file_path must be provided")

        # Extract country code from trino catalog (e.g., rpl_at -> at)
        country_code = sql_instance_country_query['trino_catalog'].split('_')[1]

        # Determine which cluster the country belongs to
        cluster = "nl_cluster" if country_code in repstat_dbs["nl_cluster"]["dbs"] else "us_cluster"
        cluster_config = repstat_dbs[cluster]

        # Create database connection parameters
        db_params = {
            "credential_key": cluster_config['credential_key'],
            "host": cluster_config['host'],
            "database": country_code,
            "db_type": cluster_config['db_type']
        }

        context.log.info(f"Saving data to replica database: {country_code} in cluster {cluster}")

        db_manager = DatabaseConnectionManager()
        engine = db_manager.get_sqlalchemy_engine(**db_params)
        connection = engine.raw_connection()

        query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
                ORDER BY ordinal_position;
                """
        try:
            with connection.cursor() as cur:
                cur.execute(query)
                columns = cur.fetchall()
                column_order = [column[0] for column in columns]

                if file_path is not None:
                    context.log.debug(f"Loading data from parquet file: {file_path}")
                    df = pd.read_parquet(file_path, dtype_backend="pyarrow")
                else:
                    context.log.debug("Loading data from DataFrame...")

                df = df.reindex(columns=column_order)  # reorder columns to original order

                csv_string = df.to_csv(index=False)

                context.log.debug(f"Copying data to >> [{schema}.{table_name}] ...")
                cur.copy_expert(
                    f"COPY {schema}.{table_name} FROM STDIN WITH (FORMAT CSV, HEADER)",
                    StringIO(csv_string)
                )

            connection.commit()
            context.log.info(f"Data successfully copied to replica database {country_code} >> [{schema}.{table_name}]")
        except Exception as e:
            context.log.error(f"Error saving data to replica database {country_code} >> [{schema}.{table_name}]: {e}")
            raise e
        finally:
            connection.close()

    @staticmethod
    def save_chunks_concurrently(
        context,
        sql_instance_country_query: dict[str, any],
        schema: str,
        table_name: str,
        chunks: List[pd.DataFrame],
        max_workers: int = 10
    ) -> int:
        """
        Saves multiple DataFrame chunks concurrently to a specified table in the replica database.

        Args:
            context: Dagster context for logging
            sql_instance_country_query: Dictionary containing SQL instance details and query information
            schema: Schema name of the target table
            table_name: Name of the target table
            chunks: List of DataFrame chunks to save
            max_workers: Maximum number of concurrent worker threads (default: 10)

        Returns:
            int: Total number of rows processed successfully
        """
        from concurrent.futures import ThreadPoolExecutor
        import concurrent.futures

        # Limit concurrent connections based on available chunks
        max_workers = min(max_workers, len(chunks))

        # Define the save function for concurrent execution
        def save_chunk(chunk_df: pd.DataFrame) -> int:
            try:
                TrinoOperations.save_to_replica_copy_method(
                    context=context,
                    sql_instance_country_query=sql_instance_country_query,
                    schema=schema,
                    table_name=table_name,
                    df=chunk_df
                )
                return len(chunk_df)
            except Exception as e:
                context.log.error(f"Error saving chunk: {str(e)}")
                return 0

        # Track total rows processed
        total_processed = 0

        # Process chunks concurrently using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks for processing
            future_to_chunk = {
                executor.submit(save_chunk, chunk): chunk
                for chunk in chunks
            }

            # Process completed futures as they finish
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    chunk_size = future.result()
                    total_processed += chunk_size
                    context.log.info(f"Successfully saved chunk with {chunk_size} rows")
                except Exception as e:
                    context.log.error(f"Error processing chunk: {str(e)}")

        context.log.info(f"Concurrent processing complete. Successfully processed {total_processed} rows")
        return total_processed
