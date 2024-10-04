import argparse
from datetime import datetime

def parse_args():
    """
    Parse command-line arguments for the script.

    Returns:
        dict: A dictionary containing the run type and date.
    """
    # Create the parser
    parser = argparse.ArgumentParser(description="Script to perform operations based on specified run type and date.")

    # Add arguments
    parser.add_argument("--run_type", type=str, required=True, choices=["init", "update"],
                        help="Type of run (either 'init' or 'update').")
    parser.add_argument("--date", type=str, required=False,
                        help="Date for the operation in YYYY-MM-DD format. Defaults to today's date if not provided.")

    # Parse the arguments
    args = parser.parse_args()

    # Validate the date format if provided
    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")  # Check date format
        except ValueError:
            parser.error("Invalid date format. Please use YYYY-MM-DD format.")

    return args  # Return arguments as a dictionary
