import logging

from dishes_forecasting.paths import PROJECT_DIR

# Create a logger
logger = logging.getLogger(__name__)

# Set the level of the logger
logger.setLevel(logging.INFO)

# Create a file handler for the logger
file_handler = logging.FileHandler(f"{PROJECT_DIR}/dishes-forecasting.log")

# Set a format for the file handler
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(name)s : %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)
