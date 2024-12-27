""" Loggin system configuration """

import logging


def setup_logger(level: str = "INFO") -> None:
    """Logging system

    Parameters
    ----------
    level: str
        The debug level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')

    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
