import logging
import sys
import os


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application."""
    # Set up root logger
    logger = logging.getLogger('stream-etl')
    logger.setLevel(level)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    
    # Add file handler for errors
    os.makedirs('logs', exist_ok=True)
    error_handler = logging.FileHandler('logs/stream_etl_errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
    
    # Create debug log file if level is DEBUG
    if level == logging.DEBUG:
        debug_handler = logging.FileHandler('logs/stream_etl_debug.log')
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(formatter)
        logger.addHandler(debug_handler)
