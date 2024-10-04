from pathlib import Path
from src.utils.constants import INTEGRATION_DATE

# Get the current file's directory
current_path = Path(__file__).resolve().parent

# Define the project path by navigating up the directory structure
# The project path is set to two levels up from the current file
project_path = current_path.parents[1]

# Optional: Print or log the paths for debugging purposes
# Uncomment the following lines to see the paths when executing the script
# print(f"Current Path: {current_path}")
# print(f"Project Path: {project_path}")
