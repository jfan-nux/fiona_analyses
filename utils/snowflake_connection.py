import os
import datetime
from typing import Optional, Union
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from utils.logger import get_logger
logger = get_logger(__name__)
try:
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine, sql
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    logger.warning("snowflake.sqlalchemy or sqlalchemy not available. SQL Alchemy functionality will be disabled.")
    SQLALCHEMY_AVAILABLE = False

# Optional PySpark imports
try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql.functions import col as _col
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType,
        TimestampType, DateType, ArrayType, MapType, StructType, DecimalType,
        ByteType, ShortType, BinaryType, NullType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    logger.warning("pyspark not available. Spark functionality will be disabled.")
    PYSPARK_AVAILABLE = False
    # Define a fallback type alias to avoid NameError
    SparkDataFrame = object  # Use object as a fallback type

# Optional polars import
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    logger.warning("polars not available. Polars functionality will be disabled.")
    POLARS_AVAILABLE = False

class SnowflakeHook:
    # Class-level variable to store persistent Spark session
    _persistent_spark_session = None

    @classmethod
    def get_or_create_spark_session(cls, app_name: str = "SnowflakeHook",
                                   local_mode: bool = True,
                                   additional_configs: Optional[dict] = None,
                                   force_new: bool = False):
        """
        Get an existing persistent Spark session or create a new one.

        Args:
            app_name: Name for the Spark application (default: "SnowflakeHook")
            local_mode: Whether to create a local-mode session with minimal networking (default: True)
            additional_configs: Additional Spark configuration parameters as dict (optional)
            force_new: Force creation of a new session instead of reusing existing (default: False)

        Returns:
            pyspark.sql.SparkSession: Spark session
        """
        # Return existing session if available and not forcing new creation
        if not force_new and cls._persistent_spark_session is not None:
            logger.info("Reusing existing persistent Spark session")
            return cls._persistent_spark_session

        # Create a new session
        try:
            logger.info("Creating new persistent Spark session")
            session = cls.create_optimized_spark_session(
                app_name=app_name,
                local_mode=local_mode,
                additional_configs=additional_configs
            )

            # Store as the persistent session
            cls._persistent_spark_session = session
            return session
        except Exception as e:
            logger.error(f"Failed to create persistent Spark session: {str(e)}")
            raise

    def __init__(
        self,
        spark = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        create_local_spark: bool = True,
        spark_config: Optional[dict] = None,
        use_persistent_spark: bool = False,
        insecure_mode: bool = True,
    ):
        """
        Instantiate snowflake hook with connection parameters.

        Args:
            spark: Spark session (optional)
            database: Database name (optional, defaults to SNOWFLAKE_DATABASE env var)
            schema: Schema name (optional, defaults to SNOWFLAKE_SCHEMA env var)
            warehouse: Warehouse name (optional, defaults to SNOWFLAKE_WAREHOUSE env var)
            role: Role name (optional, defaults to SNOWFLAKE_ROLE env var)
            username: Username (optional, defaults to SNOWFLAKE_USER env var)
            password: Password (optional, defaults to SNOWFLAKE_PASSWORD env var or keychain)
            env_file: Path to .env file (optional, defaults to 'config/.env' relative to workspace root)
            create_local_spark: Whether to create a Spark session in local mode with optimized settings
            spark_config: Additional Spark configuration parameters (optional)
            use_persistent_spark: Whether to use a persistent Spark session (default: False)
            insecure_mode: Whether to use insecure mode for certificate validation (default: True)
        """
        # First check for environment variables from shell profile for username and password
        # These take highest priority
        env_file_path = Path(__file__).parent.parent / "config" / ".env"
        load_dotenv(dotenv_path=env_file_path, override=True)
        self.user = username or os.getenv("SNOWFLAKE_USER")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE", "proddb")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "public")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "ADHOC")
        self.role = role or os.getenv("SNOWFLAKE_ROLE", "read_only_users")
        self.account = os.getenv("SNOWFLAKE_ACCOUNT", "doordash")
        self.use_persistent_spark = use_persistent_spark
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")

        # Validate required parameters
        self._validate_params()
        self.params = dict(
            user=self.user,
            password=self.password,
            schema=self.schema,
            account=self.account,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            insecure_mode=insecure_mode,
        )

        # Initialize connection attributes
        self.conn = None
        self.cursor = None

        # Setup Spark parameters if Spark is available
        if PYSPARK_AVAILABLE:
            # Use provided spark session or existing persistent session if requested
            if spark is not None:
                self.spark = spark
                logger.info("Using provided Spark session")
            elif use_persistent_spark:
                # Use or create a persistent Spark session
                self.spark = self.get_or_create_spark_session(
                    app_name="SnowflakeHook",
                    local_mode=create_local_spark,
                    additional_configs=spark_config
                )
            else:
                try:
                    self.spark = self.create_optimized_spark_session(
                        app_name="SnowflakeHook",
                        local_mode=create_local_spark,
                        additional_configs=spark_config
                    )
                except Exception as e:
                    logger.error(f"Failed to create Spark session: {str(e)}")
                    self.spark = None

            # Setup Snowflake connection parameters for Spark
            if self.spark is not None:
                self.sfparams = dict(
                    sfUrl=f"{self.account}.snowflakecomputing.com",
                    sfAccount=self.account,
                    sfUser=self.user,
                    sfPassword=self.password,
                    sfDatabase=self.database,
                    sfSchema=self.schema,
                    sfWarehouse=self.warehouse,
                    sfRole=self.role
                )
        else:
            self.spark = None

    def _validate_params(self):
        """
        Validate that all required connection parameters are present.

        Raises:
            ValueError: If any required parameter is missing.
        """
        required_params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }

        missing_params = [param for param, value in required_params.items() if not value]
        if missing_params:
            raise ValueError(f"Missing required Snowflake connection parameters: {', '.join(missing_params)}")

    def connect(self):
        """
        Establish a connection to Snowflake.

        Returns:
            The Snowflake connection.

        Raises:
            Exception: If connection fails.
        """
        try:
            self.conn = snowflake.connector.connect(**self.params)
            logger.info("Successfully connected to Snowflake")
            return self.conn
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {str(e)}")
            raise

    def close(self):
        """Close the Snowflake connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Snowflake connection closed")

    @staticmethod
    def create_optimized_spark_session(app_name: str = "SnowflakeHook",
                                       local_mode: bool = True,
                                       additional_configs: Optional[dict] = None):
        """
        Create an optimized Spark session for working with Snowflake.
        """
        if not PYSPARK_AVAILABLE:
            raise RuntimeError("PySpark is not available. Please install it with 'pip install pyspark'")

        try:
            builder = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")

            # Add network-specific configurations to help with connection issues
            builder = builder \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "120s") \
                .config("spark.storage.blockManagerSlaveTimeoutMs", "600000") \
                .config("spark.executor.instances", "1") \
                .config("spark.task.maxFailures", "10")

            # If local mode, configure specifically for local operation with minimal networking
            if local_mode:
                builder = builder \
                    .config("spark.master", "local[*]") \
                    .config("spark.driver.bindAddress", "127.0.0.1") \
                    .config("spark.driver.host", "127.0.0.1")

            # Add any additional user-provided configurations
            if additional_configs:
                for key, value in additional_configs.items():
                    builder = builder.config(key, value)

            # Create the session
            spark = builder.getOrCreate()

            # Set logging level to WARN to reduce noise
            spark.sparkContext.setLogLevel("WARN")

            logger.info(f"Optimized Spark session created successfully. Spark version: {spark.version}")
            return spark
        except Exception as e:
            logger.error(f"Failed to create optimized Spark session: {str(e)}")
            raise

    def query_snowflake(self, query: str, method: Optional[str] = 'pandas'):
        """
        Execute a query against Snowflake.

        Args:
            query: SQL query to execute
            method: Query execution method:
                - 'pandas': Uses the Snowflake connector with pandas (default)
                - 'spark': Uses PySpark with optimized network settings for local execution
                - 'polars': Uses Polars DataFrame library (if available)

        Returns:
            pandas.DataFrame, pyspark.sql.DataFrame, polars.DataFrame: Query results
            Return type depends on the method parameter
        """

        if method == 'spark' and PYSPARK_AVAILABLE and self.spark is not None:
            # Spark method (only if available)
            try:
                logger.info(f"Executing query (spark): {query[:100]}...")
                df = self.spark.read.format("snowflake")\
                    .options(**self.sfparams)\
                    .option("query", query)\
                    .load()

                # Convert column names to lowercase
                df = df.select([_col(c).alias(c.lower()) for c in df.columns])
                return df
            except Exception as e:
                logger.error(f"Error executing spark query: {str(e)}")
                raise

        elif method == 'polars' and POLARS_AVAILABLE and SQLALCHEMY_AVAILABLE:
            # Polars method (only if available)
            try:
                logger.info(f"Executing query (polars): {query[:100]}...")
                with create_engine(URL(**self.params)).connect() as ctx:
                    df = pl.read_database(sql.text(query), ctx)
                    df = df.rename({col: col.lower() for col in df.columns})
                    return df
            except Exception as e:
                logger.error(f"Error executing polars query: {str(e)}")
                raise
        else:
            # Pandas method
            try:
                # Connect if not already connected
                if not self.conn:
                    self.connect()

                # Execute query
                logger.info("Executing query (pandas)")
                self.cursor = self.conn.cursor()
                self.cursor.execute(query)
                df = self.cursor.fetch_pandas_all()

                # Convert column names to lowercase
                df.columns = map(str.lower, df.columns)
                return df
            except Exception as e:
                logger.error(f"Error executing pandas query: {str(e)}")
                raise

    def query_without_result(self, query: str):
        """
        Run a query without returning a result.

        Args:
            query: SQL query to execute
        """
        try:
            # Connect if not already connected
            if not self.conn:
                self.connect()
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    def grant_access(self, table):
        """Grant read access to users and admin access to sysadmin for a given table."""
        self.query_without_result(f"GRANT SELECT ON {table} TO ROLE read_only_users")
        self.query_without_result(f"GRANT ALL ON {table} TO ROLE sysadmin")

    def __enter__(self):
        """
        Enable the SnowflakeHook to be used as a context manager.

        Returns:
            SnowflakeHook: The current instance.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up resources when exiting a with block.

        Args:
            exc_type: Exception type if any occurred.
            exc_val: Exception value if any occurred.
            exc_tb: Exception traceback if any occurred.
        """
        # Close Snowflake connection
        self.close()

        # Don't stop spark session if it's persistent
        if PYSPARK_AVAILABLE and self.spark is not None and not self.use_persistent_spark:
            if self.spark is not SnowflakeHook._persistent_spark_session:
                self.spark.stop()

        return False  # Re-raise any exceptions that occurred

    def write_to_snowflake(self, df, table_name: str, mode: str = "append", method: str = "pandas"):
        """
        Write a DataFrame to a Snowflake table.

        Args:
            df: DataFrame to write (pandas, Spark, or polars)
            table_name: Name of the target table
            mode: Write mode (append, overwrite, error, ignore)
            method: Method to use:
                - 'pandas': Uses the Snowflake connector with pandas (default)
                - 'spark': Uses PySpark with optimized network settings
                - 'polars': Uses Polars DataFrame library (if available)

        Returns:
            bool: True if successful, False otherwise

        """
        if method == 'pandas':
            # Write using pandas
            try:
                if not self.conn:
                    self.connect()

                logger.info(f"Writing DataFrame to Snowflake table {table_name} using pandas")
                success, num_chunks, num_rows, output = write_pandas(
                    conn=self.conn,
                    df=df,
                    table_name=table_name,
                    database=self.database,
                    schema=self.schema,
                    quote_identifiers=False
                )
                self.grant_access(table_name)
                logger.info(f"Successfully wrote {num_rows} rows to {table_name}")
                return success
            except Exception as e:
                logger.error(f"Error writing DataFrame to Snowflake using pandas: {str(e)}")
                raise

        elif method == 'spark' and PYSPARK_AVAILABLE and self.spark is not None:
            # Write using Spark
            try:
                logger.info(f"Writing DataFrame to Snowflake table {table_name} using Spark")

                # Handle different input DataFrame types
                if isinstance(df, pd.DataFrame):
                    # Convert pandas to Spark
                    spark_df = self.spark.createDataFrame(df)
                elif not isinstance(df, SparkDataFrame):
                    raise ValueError("DataFrame must be a pandas DataFrame or Spark DataFrame when using 'spark' method")
                else:
                    spark_df = df

                # Write to Snowflake
                spark_df.write \
                    .format("snowflake") \
                    .options(**self.sfparams) \
                    .option("dbtable", table_name) \
                    .mode(mode) \
                    .save()
                self.grant_access(table_name)
                logger.info(f"Successfully wrote DataFrame to {table_name} using Spark")
                return True
            except Exception as e:
                logger.error(f"Error writing DataFrame to Snowflake using Spark: {str(e)}")
                raise
        else:
            # Default to pandas if method not supported
            if method != 'pandas':
                logger.warning(f"Method '{method}' not supported or required packages not available. Using pandas instead.")

            return self.write_to_snowflake(df, table_name, mode, method='pandas')

    def infer_create_table(self, df: Union[pd.DataFrame, SparkDataFrame], table_name: str,
                           schema: Optional[str] = None, database: Optional[str] = None) -> tuple:
        """
        Infer a CREATE TABLE statement and prepare the data for upload from a DataFrame.

        Args:
            df: DataFrame to analyze (pandas or Spark)
            table_name: Name of the target table
            schema: Schema name to use (defaults to self.schema if None)
            database: Database name to use (defaults to self.database if None)

        Returns:
            tuple: (create_table_sql, prepared_dataframe)

        Raises:
            ValueError: If DataFrame has duplicate column names
        """
        # Use class schema/database if not provided
        schema = schema or self.schema
        database = database or self.database

        if schema is None or database is None:
            raise ValueError("Schema and database must be provided either in method arguments or during SnowflakeHook initialization")

        # Start building the create table statement
        fully_qualified_table = f"{database}.{schema}.{table_name}"
        create_table = f"CREATE OR REPLACE TABLE {fully_qualified_table} ("

        # Process based on DataFrame type
        if isinstance(df, pd.DataFrame):
            # Check for duplicate columns
            cols = list(df.columns)
            if len(cols) != len(set(cols)):
                raise ValueError("DataFrame has duplicate column names")

            # Create a copy for uploads
            df_to_upload = df.copy()
            timestamp_cols = []

            # Add each column with appropriate type
            for i, (col, dtype) in enumerate(df.dtypes.items()):
                # Convert pandas dtype to Snowflake type
                if pd.api.types.is_object_dtype(dtype):
                    sf_type = "STRING"
                elif pd.api.types.is_integer_dtype(dtype):
                    sf_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sf_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(dtype):
                    sf_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sf_type = "TIMESTAMP_NTZ"
                    timestamp_cols.append(col)
                elif pd.api.types.is_timedelta64_dtype(dtype):
                    sf_type = "TIME"
                else:
                    # Default to string for unrecognized types
                    sf_type = "STRING"
                    logger.warning(f"Column '{col}' has unrecognized dtype {dtype}, using STRING")

                # Add column definition without quotes to avoid case-sensitivity issues
                create_table += f"{col} {sf_type}"

                # Add comma if not the last column
                if i < len(df.columns) - 1:
                    create_table += ", "

            # Close the parenthesis
            create_table += ")"

            # Handle timestamp columns for upload
            for col in timestamp_cols:
                df_to_upload[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

            return create_table, df_to_upload

        elif PYSPARK_AVAILABLE and isinstance(df, SparkDataFrame):
            # For Spark DataFrames
            spark_schema = df.schema

            # Check for duplicate columns
            cols = df.columns
            if len(cols) != len(set(cols)):
                raise ValueError("DataFrame has duplicate column names")

            # Add each column with appropriate type
            for i, field in enumerate(spark_schema.fields):
                col_name = field.name
                data_type = field.dataType

                # Convert Spark type to Snowflake type
                if isinstance(data_type, StringType):
                    sf_type = "STRING"
                elif isinstance(data_type, (IntegerType, LongType, ByteType, ShortType)):
                    sf_type = "INTEGER"
                elif isinstance(data_type, (FloatType, DoubleType)):
                    sf_type = "FLOAT"
                elif isinstance(data_type, DecimalType):
                    # Map DecimalType to NUMBER with precision and scale
                    precision = data_type.precision
                    scale = data_type.scale
                    sf_type = f"NUMBER({precision},{scale})"
                elif isinstance(data_type, BooleanType):
                    sf_type = "BOOLEAN"
                elif isinstance(data_type, TimestampType):
                    sf_type = "TIMESTAMP_NTZ"
                elif isinstance(data_type, DateType):
                    sf_type = "DATE"
                elif isinstance(data_type, BinaryType):
                    sf_type = "BINARY"
                elif isinstance(data_type, ArrayType):
                    sf_type = "ARRAY"
                elif isinstance(data_type, MapType):
                    sf_type = "OBJECT"
                elif isinstance(data_type, StructType):
                    sf_type = "OBJECT"
                elif isinstance(data_type, NullType):
                    sf_type = "STRING"  # Use STRING for null types as fallback
                else:
                    # Default to string for unrecognized types
                    sf_type = "STRING"
                    logger.warning(f"Column '{col_name}' has unrecognized Spark type {data_type}, using STRING")

                # Add column definition without quotes to avoid case-sensitivity issues
                create_table += f"{col_name} {sf_type}"

                # Add comma if not the last column
                if i < len(spark_schema.fields) - 1:
                    create_table += ", "

            # Close the parenthesis
            create_table += ")"

            # For Spark, we don't need to modify the DataFrame before upload
            return create_table, df
        else:
            raise TypeError("Input must be a pandas DataFrame or a Spark DataFrame")

    def create_and_populate_table(self, df: Union[pd.DataFrame, SparkDataFrame], table_name: str,
                                 schema: Optional[str] = None, database: Optional[str] = None,
                                 method: Optional[str] = None) -> bool:
        """
        Create a new table based on DataFrame schema and populate it with data.

        Args:
            df: DataFrame to analyze and upload (pandas or Spark)
            table_name: Name of the target table
            schema: Schema name to use (defaults to self.schema if None)
            database: Database name to use (defaults to self.database if None)
            method: Method to use for data upload ('pandas' or 'spark').
                   If None, auto-detects based on DataFrame type.

        Returns:
            bool: True if successful

        Raises:
            ValueError: If DataFrame has duplicate column names or other validation errors
        """
        try:
            # Auto-detect method based on DataFrame type if not specified
            if method is None:
                if isinstance(df, pd.DataFrame):
                    method = "pandas"
                elif PYSPARK_AVAILABLE and isinstance(df, SparkDataFrame):
                    method = "spark"
                else:
                    method = "pandas"  # Default fallback

            # Generate the CREATE TABLE statement and prepare the DataFrame
            create_table_sql, prepared_df = self.infer_create_table(
                df=df,
                table_name=table_name,
                schema=schema,
                database=database
            )

            self.query_without_result(create_table_sql)
            logger.info(f"Successfully created table {table_name}")
            success = self.write_to_snowflake(
                df=prepared_df,
                table_name=table_name,
                mode="append",
                method=method
            )

            return success
        except Exception as e:
            logger.error(f"Error creating and populating table {table_name}: {str(e)}")
            raise

    def fetch_pandas_all(self, query, params=None):
        """
        Directly execute a query and fetch all results as a pandas DataFrame.

        This is a convenience method that combines execute_query and fetch_pandas_all.

        Args:
            query (str): SQL query to execute
            params (dict, optional): Parameters to bind to the query

        Returns:
            pandas.DataFrame: Query results as a DataFrame
        """
        if not self.conn:
            self.connect()

        # Ensure we have a cursor
        if not hasattr(self, 'cursor') or self.cursor is None:
            self.cursor = self.conn.cursor()
        self.cursor.execute(query, params)
        return self.cursor.fetch_pandas_all()

    def drop_table(self, table_name: str):
        """
        Drop a table from Snowflake.

        Args:
            table_name: Name of the table to drop
        """
        self.query_without_result(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"Successfully dropped table {table_name}")