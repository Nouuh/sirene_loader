import yaml
import logging
from pathlib import Path
from src.jobs.job_runner import run_job
from src.utils.path_utils import project_path
from src.utils.args_parser import parse_args

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file: Path) -> dict:
    """Load configuration from a YAML file."""
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_file}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise

def main():
    # Parse command-line arguments (run type and date)
    args = parse_args()

    # Load configuration
    config_file = Path("config/config.yml")
    config = load_config(config_file)

    # Update configuration with command-line arguments
    config["run_type"] = args.run_type
    config["date"] = args.date

    # Log the configuration
    logger.info(f"Running job with configuration: {config}")

    # Run the job
    run_job(config)

if __name__ == "__main__":
    main()
