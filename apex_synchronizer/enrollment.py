from abc import ABC, abstractmethod
from collections import defaultdict, KeysView
from typing import Iterable, Set, Union
import logging

from .apex_data_models import ApexStudent, ApexClassroom
from .apex_session import ApexSession
from .exceptions import ApexObjectNotFoundException
from .ps_agent import fetch_enrollment, fetch_students
from .utils import flatten_ps_json
from .apex_session import TokenType


class BaseEnrollment(ABC):
    """
    Defines a common interface to which the enrollment information from
    both Apex and PowerSchool can adhere.
    """

    def __init__(self):
        self.logger = logging.getLogger('.'.join([__name__,
                                                  self.__class__.__name__]))

    def get_classrooms(self, eduid: Union[int, str, ApexStudent]) -> Set[int]:
        """
        Returns all classrooms in which a given student is enrolled.
        Students are indexed by their EDUIDs, which may be given as an
        int or a numeric string.

        :param Union[int, str] eduid: the EDUID of a given student
        :return: all classrooms in which the student is enrolled
        :rtype: set[int]
        """
        if type(eduid) is ApexStudent:
            eduid = eduid.import_user_id
        return self.student2classrooms[int(eduid)]

    def get_roster(self, section_id: Union[int, str, ApexClassroom]) -> Set[int]:
        """
        Returns the roster of a given classroom, indexed by its section
        ID. Section IDs may be given as integers or numeric strings.

        :param Union[int, str] section_id: the section ID of the
            classroom
        :return: the EDUIDs of all students in the classroom
        :rtype: set[int]
        """
        if type(section_id) is ApexClassroom:
            section_id = section_id.import_classroom_id
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
        """Returns a complete roster encompassing all students."""
        return self.student2classrooms.keys()

    @property
    def classrooms(self) -> KeysView:
        """Returns all classrooms."""
        return self.classroom2students.keys()


class PSEnrollment(BaseEnrollment):

    def __init__(self, ps_json=None):
        super().__init__()
        if ps_json is None:
            self.logger.debug('Fetching enrollment')
            ps_json = fetch_enrollment()

        self._parse_enrollment_json(map(flatten_ps_json, ps_json))
        self._parse_student_json(map(flatten_ps_json, fetch_students()))

    def _parse_enrollment_json(self, json_obj: Iterable[dict]):
        """
        Parses a JSON object returned by the `fetch_enrollment`
        function.
        """
        self._student2classrooms = defaultdict(set)
        self._classroom2students = defaultdict(set)

        self.logger.info('Iterating over enrollment.')
        for i, entry in enumerate(json_obj):
            eduid = int(entry['eduid'])
            sec_id = int(entry['section_id'])
            self.logger.debug(f'student {i}:eduid={eduid},section_id={sec_id}')

            self.student2classrooms[eduid].add(sec_id)
            self.classroom2students[sec_id].add(eduid)

        self._student2classrooms = dict(self.student2classrooms)
        self._classroom2students = dict(self.classroom2students)

    def _parse_student_json(self, json_obj: Iterable[dict]):
        """
        Parses a JSON object returned by the `fetch_students` function.
        """
        for entry in json_obj:
            eduid = entry['eduid']
            if eduid and eduid not in self._student2classrooms.keys():
                self._student2classrooms[int(eduid)] = set()

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms


class ApexEnrollment(BaseEnrollment):

    def __init__(self, access_token: TokenType = None):
        super().__init__()
        if access_token is None:
            session = ApexSession()
            access_token = session.access_token

        self.logger.info('Retrieving Apex student information from Apex API.')
        self.apex_students = ApexStudent.get_all(access_token, ids_only=True)
        self.apex_classrooms = ApexClassroom.get_all()
        self.logger.info('Retrieved Apex student information')
        self.logger.debug('Creating ApexStudent index')
        self._apex_index = {int(student.import_user_id): student
                            for student in self.apex_students}

        self._student2classrooms = {}
        self._classroom2students = {}

        n_students = len(self.apex_students)
        logging.info(f'Getting enrollment info for {n_students} students.')
        student: ApexStudent
        for i, student in enumerate(self.apex_students):
            progress = f'student {i + 1}/{n_students}'
            try:
                classrooms = student.get_enrollments(access_token)
                by_id = set([int(c.import_classroom_id) for c in classrooms])
            except ApexObjectNotFoundException:
                logging.info(f'{progress}:could not find student with EDUID '
                             f'{student.import_user_id} in PS.')
                by_id = set()
            self._student2classrooms[int(student.import_user_id)] = by_id
            self.logger.info(f'{progress}:1/2:got classrooms for student '
                             + str(student.import_user_id))

            for classroom in by_id:
                self._classroom2students[classroom] = int(student.import_user_id)
            self.logger.info(f'{progress}:2/2:created reverse mapping for student '
                             + str(student.import_user_id))

        self.logger.debug('ApexEnrollment object created successfully.')

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms

    def get_student(self, eduid: Union[str, int]):
        return self._apex_index[int(eduid)]
