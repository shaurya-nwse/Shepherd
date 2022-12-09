import os
from functools import cached_property
from typing import MutableMapping, Union
import logging
from rich.logging import RichHandler, Console


class RequiredParameterException(Exception):
    """Required parameter"""


class ConfigTypeCastException(Exception):
    """Exception while casting config parameters"""


class AppConfig:
    """Application Configuration"""

    DEBUG: bool
    BOOTSTRAP_SERVERS: str
    TOPIC: str
    GROUP_ID: str
    AUTO_OFFSET_RESET: str = "earliest"

    STREAM_NAME: str
    BATCH_SIZE: int = 10
    BATCH_TIME: int = 5
    MAX_RETRIES: int = 0  # TODO: Change to 5
    NUM_THREADS: int = 4

    def __init__(self, env: MutableMapping):
        for field, T in self.__annotations__.items():
            default_value = getattr(self, field, None)
            if default_value is None and env.get(field) is None:
                raise RequiredParameterException(
                    f"{field} is a required field."
                )

            try:
                value = env.get(field, default_value)
                if T == bool:
                    value = self._parse_bool(value)
                else:
                    value = T(value)
                self.__setattr__(field, value)
            except ValueError as e:
                raise ConfigTypeCastException(
                    f"Could not cast {field} to {T}."
                ) from e

    @cached_property
    def logger(self):
        """Cached logger"""
        return self._get_logger()

    @staticmethod
    def _parse_bool(val: Union[str, bool]) -> bool:
        return (
            val
            if isinstance(val, bool)
            else val.lower() in ("true", "yes", "1")
        )

    @staticmethod
    def _get_logger() -> logging.Logger:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # noinspection PyTypeChecker
        console_handler = RichHandler(
            markup=True, locals_max_length=None, console=Console(width=255)
        )
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(levelname)s - %(asctime)s - %(name)s - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s"
        )

        console_handler.setFormatter(fmt=formatter)
        logger.addHandler(hdlr=console_handler)
        return logger

    def __repr__(self):
        return str(self.__dict__)


config = AppConfig(os.environ)
