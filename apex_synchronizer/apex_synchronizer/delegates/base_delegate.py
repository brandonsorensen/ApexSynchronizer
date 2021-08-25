from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..apex_synchronizer import ApexSynchronizer


class SyncDelegate(ABC):

    def __init__(self, synchronizer: ApexSynchronizer):
        """
        Abstract base class that outlines behavior common to all the
        delegate classes.

        Stores reference to the parent synchronizer and sets the class's
        logger to that of said synchronizer. Requires definition of a
        `execute` method to be called with the `__call__ magic method
        such that each delegate behaves more or less as if it were a
        function defined the `ApexSynchronizer` class.

        :param ApexSynchronizer synchronizer: the parent synchronizer
        """
        self.sync = synchronizer
        self.logger = self.sync.logger  # For convenience

    @abstractmethod
    def execute(self):
        """The main logic for running a sync of this delegate's object."""
        pass

    def __call__(self):
        """Calls the main sync function."""
        self.execute()

