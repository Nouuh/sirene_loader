from pyspark.sql.functions import lit
from src.api.api_client import ApiClient
from src.delta.delta_manager import DeltaManager
from src.utils.constants import INTEGRATION_DATE
from src.utils.logger_utils import logger
from src.jobs.spark_session_maneger import SparkSessionManager

def run_job(config):
    """
    Main function to run the job for fetching data, processing it, and updating the Delta table.

    Args:
        config (dict): Configuration dictionary for the job.
    """
    logger.info("Job start ... ")

    # Fetch data from API
    api_client = ApiClient(config)
    source_path = api_client.fetch_data(None)

    # Manage Spark session
    spark_manager = SparkSessionManager(config)

    # Read the data uploaded from opendatasoft
    df_source = spark_manager.read_data(str(source_path))
    
    # Process the data
    df_transformed = df_source.withColumn('INTEGRATION_DATE', lit(INTEGRATION_DATE))
    
    # Update Delta table
    delta_manager = DeltaManager(spark_manager.spark, config)
    delta_manager.save(df_transformed)

    # Stop the Spark session
    spark_manager.stop()
