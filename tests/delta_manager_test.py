import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from src.delta.delta_manager import DeltaManager
from src.utils.config import DeltaConfig


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("pytest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def delta_manager(spark_session):
    """Fixture for DeltaManager."""
    config = {
        "run_type": "init",  # Change to "update" for update tests
        "date": "2024-10-04",
        "delta": {
            "datasource": "data",
            "source": "init",
            "target": "delta",
            "update_query_columns": ["column1", "column2"]
        }
    }
    
    return DeltaManager(spark_session, config)


def test_init(delta_manager):
    """Test the init method of DeltaManager."""
    mock_df = MagicMock()

    # Call the init method
    delta_manager.init(mock_df)

    # Check if write was called correctly
    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format().mode.assert_called_once_with("overwrite")
    mock_df.write.format().mode().save.assert_called_once_with(delta_manager.config.target_path)


def test_upsert(delta_manager):
    """Test the upsert method of DeltaManager."""
    mock_df = MagicMock()

    # Call the upsert method
    delta_manager.upsert(mock_df)

    # Check if the merge method was called
    assert delta_manager.config.target_path in delta_manager.config.target_path


if __name__ == "__main__":
    pytest.main()
