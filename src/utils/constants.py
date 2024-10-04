from datetime import datetime
from typing import Final

# Define a constant for the integration date.
# Use Final to indicate that this value should not change.
INTEGRATION_DATE: Final[datetime] = datetime.now()  # Change this if you want a static date

# Aliases for data in the Delta table.
NEW_DATA_ALIAS: Final[str] = "new_data" 
OLD_DATA_ALIAS: Final[str] = "old_data"

# Optionally, you can include a print statement or log the constants for clarity/debugging
# This line is commented out, but can be uncommented for debugging purposes
# print(f"Integration Date: {INTEGRATION_DATE}, New Data Alias: {NEW_DATA_ALIAS}, Old Data Alias: {OLD_DATA_ALIAS}")
