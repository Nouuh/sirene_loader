import logging
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from src.utils.config import DeltaConfig
from src.utils.constants import OLD_DATA_ALIAS, NEW_DATA_ALIAS

class DeltaManager:
    """
    DeltaManager handles the initialization and updating of a Delta table.

    Attributes:
        config (DeltaConfig): Configuration object for managing Delta tables.
        spark (SparkSession): The active Spark session.
        condition (str): Logical condition used in building merge predicates (e.g., 'AND').
    """

    def __init__(self, spark: SparkSession, config):
        """
        Initialize DeltaManager with a Spark session and a DeltaConfig object.

        Args:
            spark (SparkSession): Spark session to be used for Delta table operations.
            config (DeltaConfig): Configuration for the Delta table, including paths and query parameters.
        """
        self.config = DeltaConfig(config)
        self.spark = spark
        self.condition = " AND "

    def save(self, df: DataFrame):
        """
        Save data based on the run type specified in the config (either 'init' or 'update').

        Args:
            df (DataFrame): DataFrame containing the data to save.

        Raises:
            ValueError: If an unknown run type is encountered.
        """
        logging.info(f"Run type: {self.config.run_type}")

        if self.config.run_type == "init":
            self.init(df)
        elif self.config.run_type == "update":
            self.upsert(df)
        else:
            raise ValueError(f"Unknown run type: {self.config.run_type}")

    def upsert(self, df: DataFrame):
        """
        Perform an upsert (merge) operation on the Delta table, merging new data into existing data.

        Args:
            df (DataFrame): DataFrame containing new data to merge.

        Raises:
            Exception: If the merge operation fails.
        """
        logging.info(f"Starting upsert into Delta table at {self.config.target_path}")
        try:
            delta_table = DeltaTable.forPath(self.spark, self.config.target_path)
            merge_condition = self.build_merge_predicate()
            
            delta_table.alias(OLD_DATA_ALIAS).merge(
                df.alias(NEW_DATA_ALIAS),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            logging.info("Upsert operation completed successfully.")
        except Exception as e:
            logging.error(f"Upsert failed: {e}")
            raise e

    def init(self, df: DataFrame):
        """
        Initialize a Delta table by overwriting any existing data.

        Args:
            df (DataFrame): DataFrame containing data for initialization.

        Raises:
            Exception: If the initialization operation fails.
        """
        logging.info(f"Starting Delta table initialization at {self.config.target_path}")
        try:
            df.write.format("delta").mode("overwrite").save(self.config.target_path)
            logging.info(f"Delta table initialized successfully at {self.config.target_path}")
        except Exception as e:
            logging.error(f"Table initialization failed: {e}")
            raise e

    def build_merge_predicate(self):
        """
        Build the SQL predicate used for merging new data into the Delta table.

        Returns:
            str: A SQL WHERE condition that merges data based on the config's update columns.
        """
        predicates = [f"{OLD_DATA_ALIAS}.{col} = {NEW_DATA_ALIAS}.{col}" for col in self.config.update_query_columns]
        return f" {self.condition} ".join(predicates)
