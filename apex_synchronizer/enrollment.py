from abc import ABC, abstractmethod
from collections import defaultdict, KeysView
from dataclasses import dataclass
from typing import Iterable, List, Set, Union
import logging

import requests

from . import exceptions
from .apex_data_models import ApexStudent, ApexClassroom, SCHOOL_CODE_MAP
from .apex_session import ApexSession
from .ps_agent import fetch_enrollment, fetch_students
from .utils import flatten_ps_json
from .apex_session import TokenType
import apex_synchronizer.apex_data_models as adm


class BaseEnrollment(ABC):
    """
    Defines a common interface to which the enrollment information from
    both Apex and PowerSchool can adhere.
    """

    def __init__(self, exclude=None):
        """
        :param Union[str, IO] exclude: a list, path to a file containing,
            or file object containing a list of students IDs to exempt
            from the syncing process
        """
        logger_name = '.'.join([__name__, self.__class__.__name__])
        self.logger = logging.getLogger(logger_name)
        self._all_students = set()
        if exclude is None:
            self.exclude = []
        elif isinstance(str, exclude):
            try:
                self.exclude = [l.strip() for l in open(exclude, 'r')]
            except FileNotFoundError:
                self.logger(f'Could not find file \"{exclude}\".')
                self.exclude = []
        else:
            try:
                ex_iter = exclude.readlines()
            except AttributeError:
                ex_iter = iter(exclude)

            self.exclude = [l.strip() for l in ex_iter]
        self.exclude = set(self.exclude)

    def get_classrooms(self, eduid: Union[str, ApexStudent]) -> Set[int]:
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
        try:
            return self.student2classrooms[eduid]
        except KeyError:
            return set()

    def get_roster(self, section_id: Union[int, ApexClassroom]) -> Set[str]:
        """
        Returns the roster of a given classroom, indexed by its section
        ID. Section IDs may be given as integers or numeric strings.

        :param Union[int, str] section_id: the section ID of the
            classroom
        :return: the IDs of all students in the classroom
        :rtype: set[int]
        """
        if type(section_id) is ApexClassroom:
            section_id = section_id.import_classroom_id
        try:
            return self.classroom2students[int(section_id)]
        except KeyError:
            return set()

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
        """
        Returns a complete roster encompassing all students in the form
        of student IDs.
        """
        return self.student2classrooms.keys()

    @property
    def students(self) -> Set:
        """
        Returns a complete roster encompassing all students in the form
        of data objects, i.e. `ApexStudent` or dict/JSON objects.
        """
        return self._all_students

    @property
    def classroom_ids(self) -> KeysView:
        """Returns all classroom IDs."""
        return self.classroom2students.keys()


@dataclass
class PSStudent(object):
    import_user_id: str
    import_org_id: int

    def __hash__(self):
        return hash((self.import_user_id, self.import_org_id))


@dataclass
class EnrollmentEntry(object):
    student: PSStudent
    import_classroom_id: int

    def __hash__(self):
        return hash((self.student, self.import_classroom_id))


class PSEnrollment(BaseEnrollment):

    def __init__(self, ps_json=None, exclude=None):
        super().__init__(exclude=exclude)
        if ps_json is None:
            self.logger.debug('Fetching enrollment')
            ps_json = fetch_enrollment()

        self._all_entries: Set[EnrollmentEntry] = set()
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
            org_id = int(entry['school_id'])
            sec_id = int(entry['section_id'])
            email = entry['email']
            if email in self.exclude:
                self.debug(f'Student with ID \"{email}\" in exclude list.')
                continue

            if org_id not in SCHOOL_CODE_MAP.keys():
                self.logger.debug(f'Section "{sec_id}" will not be added '
                                  'as it belongs to unrecognized org '
                                  f'"{org_id}".')
                continue
            if sec_id < 0:
                self.logger.debug(f'Section "{sec_id}" will not be added '
                                  'because it contains a negative section '
                                  f'ID: "{sec_id}".')
                continue

            student = PSStudent(email, org_id)
            enroll_entry = EnrollmentEntry(student, sec_id)
            self._all_entries.add(enroll_entry)

            self.logger.debug(f'student {i}:eduid={eduid},section_id={sec_id}')

            self.student2classrooms[email].add(sec_id)
            self._all_students.add(student)
            self.classroom2students[sec_id].add(email)

        self._student2classrooms = dict(self.student2classrooms)
        self._classroom2students = dict(self.classroom2students)

    def _parse_student_json(self, json_obj: Iterable[dict]):
        """
        Parses a JSON object returned by the `fetch_students` function.
        """
        for entry in json_obj:
            email = entry['email']
            if email in self.exclude:
                self.debug(f'Student with ID \"{email}\" in exclude list.')
                continue
            org_id = entry['school_id']
            if not all((email, org_id)):
                self.logger.debug('The following JSON object has no ID. '
                                  'Skipping.\n' + str(entry))
                continue
            org_id = int(org_id)
            grade_level = int(entry['grade_level'])
            if org_id == 615 and grade_level not in range(5, 9):
                continue
            student = PSStudent(email, org_id)
            if email and email not in self._student2classrooms.keys():
                self._student2classrooms[student.import_user_id] = set()

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms

    @property
    def classrooms(self):
        # FIXME
        raise NotImplementedError


class ApexEnrollment(BaseEnrollment):

    def __init__(self, access_token: TokenType = None,
                 session: requests.session = None,
                 student_ids: List[int] = None,
                 exclude=None):
        """

        :param access_token: An Apex access token
        :param session: an existing Session
        :param student_ids: an optional list of Apex student IDs for
            which class enrollments should be obtained. If None,
            gets all students in the Apex.
        """
        super().__init__(exclude=exclude)

        if not any([session, access_token]):
            session = ApexSession()

        if student_ids is None:
            self.logger.info('Retrieving Apex student information from Apex API.')
            self._all_students = ApexStudent.get_all(token=access_token,
                                                     session=session)
        else:
            self._all_students = ApexStudent.get_collection(student_ids,
                                                            token=access_token,
                                                            session=session)
            self.logger.info('Retrieved Apex student information')
        self.logger.debug('Creating ApexStudent index')
        self._apex_index = {student.import_user_id: student
                            for student in self._all_students
                            if student.import_user_id not in self.exclude}
        self.logger.info('Getting all Apex classrooms.')
        self._classroom_index = {int(c.import_classroom_id): c
                                 for c in ApexClassroom.get_all(session=session)}

        self.logger.info('Getting enrollment information for all relevant '
                         'students.')
        self._student2classrooms = (
            adm.apex_classroom.get_classrooms_for_eduids(set(self._apex_index),
                                                         session=session,
                                                         return_empty=True,
                                                         ids_only=True)
        )

        self._classroom2students = defaultdict(list)

        for i, (student, classrooms) in enumerate(self._student2classrooms
                                                      .items()):
            progress = f'{i + 1}/{len(self._student2classrooms)}'
            classroom: ApexClassroom
            for classroom in classrooms:
                self._classroom2students[classroom].append(student)
            self.logger.info(f'{progress}:created reverse mapping for student '
                             + str(student))

        self._classroom2students = dict(self._classroom2students)
        self.logger.debug('ApexEnrollment object created successfully.')

    @property
    def classroom2students(self) -> dict:
        return self._classroom2students

    @property
    def student2classrooms(self) -> dict:
        return self._student2classrooms

    def add_classroom(self, c: ApexClassroom):
        c_id = c.import_classroom_id
        if c_id not in self.classroom2students.keys():
            self.update_classroom(c)
            self._classroom2students[c_id] = []
            self.logger.debug(f'Successfully added classroom "{c_id}".')
        else:
            self.logger.debug(f'Classroom {c_id} not added because it already '
                              'exists.')

    def add_student(self, s: ApexStudent):
        s_id = s.import_user_id
        if s_id not in self.student2classrooms.keys():
            self.update_student(s)
            self._student2classrooms[s_id] = []
            self.logger.debug(f'Successfully added student "{s_id}" to '
                              'enrollment.')

    def add_to_classroom(self, s: ApexStudent,
                         c: Union[ApexClassroom, Iterable[ApexClassroom]],
                         must_exist: bool = False):
        """
        Adds a student to one or more classrooms.

        :param s: the student to add
        :param c: a classroom or collection of classrooms
        :param must_exist: if true, will raise an error when one of the
            classrooms is not already in the enrollment records;
            otherwise, adds such classrooms
        """
        classrooms = [c] if isinstance(c, ApexClassroom) else c

        s_id = s.import_user_id
        if not must_exist:
            self.add_student(s)

        for classroom in classrooms:
            if not must_exist:
                self.add_classroom(classroom)
            c_id = classroom.import_classroom_id
            try:
                self._classroom2students[c_id].append(s_id)
                self._student2classrooms[s_id].append(c_id)
                self.logger.debug(f'Successfully enrolled student "{s_id}" to '
                                  f'to classroom "{c_id}" in local enrollment '
                                  'context.')
            except KeyError as ke:
                raise exceptions.ApexNoEnrollmentRecord(ke.args[0]) from ke

    def disenroll(self, s: ApexStudent):
        """Removes student from all enrollments."""
        s_id = s.import_user_id
        if s_id not in self.student2classrooms.keys():
            raise exceptions.ApexNoEnrollmentRecord(s.import_user_id)

        for c_id in self.student2classrooms[s_id]:
            classroom = self.get_classroom_for_id(c_id)
            self.withdraw_student(s, classroom)

        del self._student2classrooms[s_id]
        del self._apex_index[s_id]
        self.logger.debug(f'Successfully disenrolled student "{s_id}".')

    def get_classroom_for_id(self, c_id: int) -> ApexClassroom:
        try:
            return self._classroom_index[int(c_id)]
        except ValueError:
            raise KeyError(c_id)

    def get_student_for_id(self, user_id: str) -> ApexStudent:
        return self._apex_index[user_id]

    def update_classroom(self, c: ApexClassroom):
        self._classroom_index[int(c.import_classroom_id)] = c

    def update_student(self, s: ApexStudent):
        self._apex_index[s.import_user_id] = s

    def withdraw_student(self, s: ApexStudent, c: ApexClassroom):
        """Withdraws student from a given classroom."""
        s_id = s.import_user_id
        c_id = c.import_classroom_id
        try:
            self._student2classrooms[s_id].remove(c_id)
        except KeyError:
            pass
        try:
            self._classroom2students[c_id].remove(s_id)
        except KeyError:
            pass
        self.logger.debug(f'Student "{s_id}" successfully withdrawn from '
                          'local enrollment context.')

    @property
    def classrooms(self) -> Set:
        """Returns all classroom objects."""
        return set(self._classroom_index.values())

