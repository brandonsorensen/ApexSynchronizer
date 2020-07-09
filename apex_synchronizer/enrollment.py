from abc import ABCMeta, abstractmethod
from .apex_data_models import ApexStudent
from .apex_session import ApexSession
from collections import defaultdict, KeysView
from .ps_agent import fetch_enrollment
from typing import Iterable, Set, Union
from .utils import flatten_ps_json
import logging


logger = logging.getLogger(__name__)


class BaseEnrollment(metaclass=ABCMeta):
    """
    Defines a common interface that both the enrollment information from
    Apex and PowerSchool.
    """

    def get_classrooms(self, eduid: Union[int, str]) -> Set[int]:
        """
        Returns all classrooms in which a given student is enrolled. Students
        are indexed by their EDUIDs, which may be given as an int or a
        numeric string.

        :param Union[int, str] eduid: the EDUID of a given student
        :return: all classrooms in which the student is enrolled
        :rtype: set[int]
        """
        return self.student2classrooms[int(eduid)]

    def get_roster(self, section_id: Union[int, str]) -> Set[int]:
        """
        Returns the roster of a given classroom, indexed by its section ID.
        Section IDs may be given as integers or numeric strings.

        :param Union[int, str] section_id: the section ID of the classroom
        :return: the EDUIDs of all students in the classroom
        :rtype: set[int]
        """
        return self.classroom2students[int(section_id)]

    @property
    @abstractmethod
    def classroom2students(self) -> dict:
        """A mapping from classrooms to all the students they contain."""
        pass

    @property
    @abstractmethod
    def student2classrooms(self) -> dict:
        """A mapping from all students to the classrooms they are in."""
        pass

    @property
    def roster(self) -> KeysView:
        """Returns a complete """
        return self.student2classrooms.keys()

    @property
    def classrooms(self) -> KeysView:
        """Returns all classrooms."""
        return self.classroom2students.keys()


class PSEnrollment(BaseEnrollment):

    def __init__(self, ps_json=None):
        if ps_json is None:
            ps_json = fetch_enrollment()

        json_obj = map(flatten_ps_json, ps_json)
        self._parse_json(json_obj)

    def _parse_json(self, json_obj: Iterable[dict]):
        """Parses a JSON object returned by the `fetch_enrollment` function."""
        self._student2classrooms = defaultdict(set)
        self._classroom2students = defaultdict(set)

        for entry in json_obj:
            eduid = int(entry['eduid'])
            sec_id = int(entry['section_id'])

            self.student2classrooms[eduid].add(sec_id)
            self.classroom2students[sec_id].add(eduid)

        self._student2classrooms = dict(self.student2classrooms)
        self._classroom2students = dict(self.classroom2students)

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms


class ApexEnrollment(BaseEnrollment):

    def __init__(self, access_token=None):
        if access_token is None:
            session = ApexSession()
            access_token = session.access_token

        self._apex_students = ApexStudent.get_all(access_token)
        logger.info('Retrieved Apex student information')

        self._student2classrooms = {}
        self._classroom2students = {}

        n_students = len(self._apex_students)
        logging.info(f'Getting enrollment info for {n_students} students.')
        for i, student in enumerate(self._apex_students):
            progress = f'student {i}/{n_students}'
            classrooms = student.get_enrollments(access_token)
            by_id = set([c.section_id for c in classrooms])
            self._student2classrooms[int(student.import_user_id)] = by_id
            logger.info(f'{progress}:1/2:got classrooms for student {student.import_user_id}')

            for classroom in by_id:
                self._classroom2students[classroom] = student
            logger.info(f'{progress}:2/2:created reverse mapping for student {student.import_user_id}')

        logger.debug('ApexEnrollment object created successfully.')

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms


