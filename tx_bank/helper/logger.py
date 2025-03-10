import logging


class LoggerSimple:
    """
    A simple logger utility to provide a consistent logging setup.

    This class ensures that loggers are configured properly without 
    duplicate handlers and follows a standard log format.
    """
    @staticmethod
    def get_logger(name):
        """
        Retrieves a logger instance with a standard configuration.

        - If a logger with the given `name` already exists, it is returned.
        - If no handlers exist for the logger, a new stream handler is added with
          a predefined log format.

        :param name: The name of the logger (usually `__name__`).
        :return: A configured logger instance.
        """
        logger = logging.getLogger(name)
        if not logger.hasHandlers():
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
