import abc
import json
from typing import Dict, Any


# pylint: disable=too-few-public-methods
class Deserializer(metaclass=abc.ABCMeta):
    """
    Interface for deserializers
    Deserializers are not required if there is no computation.
    We can forward a record as is. These are here for future
    extensions.
    """

    @abc.abstractmethod
    def deserialize(self, record) -> Dict[str, Any]:
        """Abstract deserialize method"""


class JsonDeserializer(Deserializer):
    """JSON deserializer"""

    def deserialize(self, record: bytes) -> Dict[str, Any]:
        return json.loads(record)


# pylint: enable=too-few-public-methods
