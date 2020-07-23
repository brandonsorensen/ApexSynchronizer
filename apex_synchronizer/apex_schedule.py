from typing import IO, Union
import json


class ApexSchedule(object):

    """
    Which of the :class:`ApexSynchronizer` methods to be run. You
    can pass an :class:`ApexSchedule` object to the
    :meth:`ApexSynchronizer.run_schedule` method to run all attributes
    set to True.

    Use the :meth:`from_json` method to build an object from a JSON
    file on the disk and the :meth:`default` method to build the object
    specified by the `_DEFAULT_SCHEDULE` class attribute.
    """

    _DEFAULT_SCHEDULE = {
        'sync_classrooms': True,
        'sync_rosters': True,
        'sync_classroom_enrollment': True,
        'sync_staff': False
    }

    def __init__(self, sync_classrooms, sync_rosters, sync_staff,
                 sync_classroom_enrollment):
        self.sync_classrooms = sync_classrooms
        self.sync_rosters = sync_rosters
        self.sync_staff = sync_staff
        self.sync_classroom_enrollment = sync_classroom_enrollment

    def __getitem__(self, item):
        try:
            return getattr(self, item)
        except AttributeError:
            return False

    def __str__(self):
        return f'{self.__class__.__name__}({str(self.to_dict())})'

    __repr__ = __str__

    @classmethod
    def default(cls) -> 'ApexSchedule':
        return cls(**cls._DEFAULT_SCHEDULE)

    @classmethod
    def from_json(cls, json_path: Union[str, IO]) -> 'ApexSchedule':
        """Creates a schedule from a JSON file."""
        if isinstance(json_path, str):
            return cls(**json.load(open(json_path, 'r')))
        with json_path:
            return cls(**json.load(json_path))

    def to_dict(self) -> dict:
        return self.__dict__


