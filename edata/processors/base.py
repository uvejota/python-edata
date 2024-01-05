"""Base definitions for processors"""

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Iterable


class Processor(ABC):
    """A base class for data processors"""

    _LABEL = "Processor"

    def __init__(self, input_data: Iterable[dict[str, Any]], auto: bool = True):
        """Init method"""
        self._input = deepcopy(input_data)
        self._output = None
        if auto:
            self.do_process()

    @abstractmethod
    def do_process(self):
        """The processing method"""

    @property
    def output(self):
        """An output property"""
        return deepcopy(self._output)
