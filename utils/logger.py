# utils/logger.py
import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(job_name):
    """
    Create a standard logger for all jobs.
    
    This logger writes to both console and a file in the logs/ directory.
    Each job gets its own timestamped log file for easy debugging.
    
    Parameters:
    -----------
    job_name : str
        Name of the job (e.g., "bronze_digest", "silver_transform")
        Used for the log filename
    
    Returns:
    --------
    logger : logging.Logger
        Configured logger instance ready to use
    
    Usage:
    ------
    logger = setup_logger("bronze_digest")
    logger.info("Processing started...")
    logger.error("Something went wrong!", exc_info=True)
    """
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{job_name}_{timestamp}.log"
    
    # Create logger with job name
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to prevent duplicates
    logger.handlers = []
    
    # Create formatters
    # Console format: simpler, for readability
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File format: more detailed, for debugging
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler - detailed logs for debugging
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)  # File gets everything
    file_handler.setFormatter(file_formatter)
    
    # Console handler - important messages only
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Console gets INFO and above
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
