"""
Logging configuration module for Google Ads Pipeline.
Provides a centralized way to configure and manage application logging.
"""
import logging
import logging.handlers
import os

class LoggerConfig:
    """A class to manage and configure application logging."""
    
    def __init__(self, name=None):
        """Initialize logger configuration.
        
        Args:
            name: Optional name for the logger. If None, uses the module name.
        """
        self.name = name
        
    def setup_logging(self, enable_console_logs=True):
        """Configure advanced logging with custom formatting and handlers.
        
        Args:
            enable_console_logs: Whether to output logs to the console. 
                                Set to False in production to reduce output.
                                
        Returns:
            A configured logger instance.
        """
        logger_name = self.name if self.name else __name__
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # Check if the logger already has handlers to prevent duplicates
        if not logger.handlers:
            # Create formatters
            detailed_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
            )
            
            # Console handler - only add if enabled
            if enable_console_logs:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(detailed_formatter)
                logger.addHandler(console_handler)
        
        return logger
    
    @staticmethod
    def get_logger_from_config(config_module, logger_name=None):
        """Get a configured logger based on environment variables or config module.
        
        Args:
            config_module: The configuration module containing ENABLE_CONSOLE_LOGS.
            logger_name: Optional name for the logger.
            
        Returns:
            A configured logger instance.
        """        # Get the enable_console_logs setting from environment variable or config
        enable_console_logs = os.environ.get("ENABLE_CONSOLE_LOGS", 
                                           getattr(config_module, "ENABLE_CONSOLE_LOGS", True))
        
        # Convert string value to boolean if needed
        if isinstance(enable_console_logs, str):
            # Convert string values to appropriate boolean
            if enable_console_logs.lower() in ("true", "1", "yes", "y", "on"):
                enable_console_logs = True
            elif enable_console_logs.lower() in ("false", "0", "no", "n", "off"):
                enable_console_logs = False
        
        # Create and configure logger
        logger_config = LoggerConfig(logger_name)
        return logger_config.setup_logging(enable_console_logs)
