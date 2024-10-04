import os
import logging
import requests
from pathlib import Path
from src.utils.config import ApiClientConfig
from src.utils.path_utils import project_path

# Configure logging at the INFO level
logging.basicConfig(level=logging.INFO)

class ApiClient:
    """
    A client to interact with an external API, fetch data, and save it to the local filesystem.

    Attributes:
        config (ApiClientConfig): Configuration object for the API client, including API URL, format, and other parameters.
    """
    
    def __init__(self, config):
        """
        Initialize the ApiClient with the provided configuration.

        Args:
            config (dict): A dictionary containing API configuration options such as `run_type`, `api_url`, `format`, and `date`.
        """
        self.config = ApiClientConfig(config)
        self.file_path = self._build_target_path()
        self.request_url = self._build_request_url()
        print(self.file_path)

    def fetch_data(self, params):
        """
        Fetch data from the API and save it to the target path as a file.

        Args:
            params (dict): Query parameters to include in the API request.

        Returns:
            str: The file path where the data is saved.

        Raises:
            Exception: If the API request fails or file writing encounters an error.
        """
        logging.info(f"Start uploading opendatasoft - url: {self.request_url}")

        try:
            response = requests.get(self.request_url, params=params)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx and 5xx status codes)
            
            logging.info("Request successful - saving file...")
            with open(self.file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            
            logging.info(f"File saved at {self.file_path}")
            return self.file_path
        
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise e

    def _build_request_url(self):
        """
        Construct the API request URL using the provided configuration.

        Returns:
            str: The constructed API request URL.
        """
        return f"{self.config.api_url}/{self.config.format}?where={self.config.update_params}>'{self.config.date}'"

    def _build_target_path(self):
        """
        Build the target file path where the fetched data will be stored.

        Returns:
            str: The absolute file path where the data will be saved.
        """
        file_name = f"sirene_{self.config.date}.csv"
        return Path(project_path) / "data" / self.config.run_type / file_name
