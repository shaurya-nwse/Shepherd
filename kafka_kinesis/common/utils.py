from typing import Type
from enum import Enum, auto


def implements_deco(proto: Type):
    """Checks if an interface is implemented"""

    def _deco(cls_def):
        try:
            assert issubclass(cls_def, proto)
        except AssertionError as e:
            e.args = (f"{cls_def} does not implement {proto}",)
            raise
        return cls_def

    return _deco


class MessageFormat(Enum):
    """Messages types"""

    JSON = auto()
    AVRO = auto()
    STRING = auto()
