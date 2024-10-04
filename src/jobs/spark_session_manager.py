from pyspark.sql import SparkSession
from src.utils.config import SparkConfig

class SparkSessionManager:
    """
    Manages the creation and configuration of a Spark session.

    Attributes:
        spark (SparkSession): The active Spark session.
    """

    def __init__(self, config):
        """
        Initialize SparkSessionManager with the provided configuration.

        Args:
            config (dict): Configuration dictionary containing Spark settings.
        """
        self.config = SparkConfig(config)
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        """
        Create and configure a Spark session based on the provided config.

        Args:
            config (dict): Configuration dictionary for Spark settings.

        Returns:
            SparkSession: Configured Spark session.
        """
        builder = SparkSession.builder

        if self.config:
            for key, value in self.config.spark_config.items():
                builder.config(key, value)
        
        spark = builder.appName("sirene_loader").getOrCreate()
        return spark

    def read_data(self, source_path):
        """
        Read data from the given source path using the configured Spark session.

        Args:
            source_path (str): Path to the data source.

        Returns:
            DataFrame: Loaded DataFrame from the source path.
        """
        return self.spark.read.format(self.config.read_format) \
                              .options(**self.config.read_options) \
                              .load(source_path)

    def stop(self):
        """Stop the Spark session."""
        self.spark.stop()
