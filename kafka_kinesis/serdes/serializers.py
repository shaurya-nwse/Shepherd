import abc
import json
from typing import TypeVar, Generic
from dataclasses import asdict, is_dataclass


T = TypeVar("T")


# pylint: disable=too-few-public-methods
class SerializationException(Exception):
    """Exception serializing object"""


class Serializer(metaclass=abc.ABCMeta):
    """Interface for serializers"""

    @abc.abstractmethod
    def serialize(self, record: T) -> bytes:
        """Serialize generic"""


class DataclassSerializer(Serializer, Generic[T]):
    """Serializer for dataclasses"""

    def serialize(self, record: T) -> bytes:
        """Serialize a dataclass to bytes"""
        if not is_dataclass(record):
            raise SerializationException(f"Type {T} is not a valid dataclass.")
        data_dict = asdict(record)
        return json.dumps(data_dict).encode()


# TODO: create class to register serializers and access via __getitem__

# pylint: enable=too-few-public-methods
