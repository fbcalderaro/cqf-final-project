import logging
import sys

# Configure the logger once when the module is imported
# This sets up logging to a file and to the console
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("trading_system.log"), # Log to a file
        logging.StreamHandler(sys.stdout)          # Also log to the console
    ]
)

# Create a logger instance that other modules can import and use
log = logging.getLogger(__name__)