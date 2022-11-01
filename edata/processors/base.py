"""Base definitions for processors"""

from abc import ABC, abstractmethod
from copy import deepcopy


class Processor(ABC):
    """A base class for data processors"""

    _LABEL = "Processor"

    def __init__(self, input, settings={}, auto=True):
        """Init method"""
        self._input = deepcopy(input)
        self._settings = settings
        self._ready = False
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
