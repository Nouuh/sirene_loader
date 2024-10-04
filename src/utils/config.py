class DeltaConfig:
    """
    Configuration class for Delta operations.

    Attributes:
        run_type (str): The type of operation (init or update).
        date (datetime): The date associated with the operation.
        datasource (str): The data source for the operation.
        source (str): The source data name.
        target (str): The target data name.
        update_query_columns (list): List of columns to update.
        source_path (str): Path to the source data.
        target_path (str): Path to the target data.
    """

    def __init__(self, config):
        self.run_type = config.get("run_type")
        self.date = config.get("date")
        
        delta_config = config.get("delta", {})
        self.datasource = delta_config.get("datasource")
        self.source = delta_config.get("source")
        self.target = delta_config.get("target")
        self.update_query_columns = delta_config.get("update_query_columns", [])
        
        # Construct paths
        self.source_path = f"{self.datasource}/{self.source}"
        self.target_path = f"{self.datasource}/{self.target}"

        # Validate run_type
        if self.run_type not in ["init", "update"]:
            raise ValueError(f"Invalid run type: {self.run_type}. Expected 'init' or 'update'.")


class SparkConfig:
    """
    Configuration class for Spark operations.

    Attributes:
        run_type (str): The type of operation (init or update).
        date (datetime): The date associated with the operation.
        spark_config (dict): Spark configuration parameters.
        read_options (dict): Options for reading data.
        read_format (str): Format of the data to read.
    """

    def __init__(self, config):
        self.run_type = config.get("run_type")
        self.date = config.get("date")
        
        spark_config = config.get("spark", {})
        self.spark_config = spark_config.get("spark_config", {})
        self.read_options = spark_config.get("read_options", {})
        self.read_format = spark_config.get("read_format", "csv")  # Default to csv if not specified
        
        # Validate run_type
        if self.run_type not in ["init", "update"]:
            raise ValueError(f"Invalid run type: {self.run_type}. Expected 'init' or 'update'.")


class ApiClientConfig:
    """
    Configuration class for API client operations.

    Attributes:
        run_type (str): The type of operation (init or update).
        date (datetime): The date associated with the operation.
        api_url (str): The URL of the API.
        format (str): Format of the API data.
        update_params (str): Parameters to update in the API.
    """

    def __init__(self, config):
        self.run_type = config.get("run_type")
        self.date = config.get("date")
        
        api_config = config.get("api", {})
        self.api_url = api_config.get("api_url")
        self.format = api_config.get("format")
        self.update_params = api_config.get("update_params")
        
        # Validate run_type
        if self.run_type not in ["init", "update"]:
            raise ValueError(f"Invalid run type: {self.run_type}. Expected 'init' or 'update'.")


