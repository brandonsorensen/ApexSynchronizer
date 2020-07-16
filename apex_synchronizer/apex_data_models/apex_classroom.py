from datetime import datetime
from requests import Response
from typing import List, Type, Union
from urllib.parse import urljoin, urlparse
import json
import logging

import requests

from .apex_data_object import ApexDataObject
from .apex_staff_member import ApexStaffMember
from .utils import BASE_URL, APEX_DATETIME_FORMAT, PS_DATETIME_FORMAT
from .. import exceptions, utils
from ..ps_agent import course2program_code, fetch_staff, fetch_classrooms
from ..utils import get_header, levenshtein_distance


class ApexClassroom(ApexDataObject):

    """
    Represents a staff member (likely a teacher) in the Apex database.

    :param Union[int, str] import_classroom_id:
                            identifier for the database, common to
                            Apex and PowerSchool
    :param Union[int, str] import_org_id: the school to which teacher
                                          staff member belongs
    :param Union[int, str] import_user_id: teacher's ImportId
    :param str classroom_name: name of the classroom
    :param List[str] product_codes: the Apex product codes representing
        which curricula are taught in the classroom
    :param str classroom_start_date: the day the classroom starts
    :param str program_code: the program to which the classroom belongs
    """

    url = urljoin(BASE_URL, 'classrooms')
    role = 'T'
    ps2apex_field_map = {
        'first_day': 'classroom_start_date',
        'teacher_id': 'import_user_id',
        'school_id': 'import_org_id',
        'section_id': 'import_classroom_id',
        'course_name': 'course_name',
        'section_number': 'section_number',
        'apex_program_code': 'product_codes'
    }
    post_heading = 'classroomEntries'

    def __init__(self, import_org_id: str, import_classroom_id: str,
                 classroom_name: str, product_codes: [str], import_user_id: str,
                 classroom_start_date: str, program_code: str):
        super().__init__(import_user_id, import_org_id)
        self.import_classroom_id = import_classroom_id
        self.classroom_name = classroom_name
        self.product_codes = product_codes
        self.classroom_start_date = classroom_start_date
        self.program_code = program_code

    @classmethod
    def from_powerschool(cls, json_obj: dict) -> 'ApexClassroom':
        kwargs = cls._init_kwargs_from_ps(json_obj)
        kwargs['classroom_name'] = kwargs['course_name'] + ' - ' + kwargs['section_number']
        del kwargs['course_name']
        del kwargs['section_number']

        kwargs['program_code'] = course2program_code[int(kwargs['import_org_id'])]
        kwargs['product_codes'] = [kwargs['product_codes']]

        return cls(**kwargs)

    def put_to_apex(self, token, main_id='ImportClassroomId') -> Response:
        return super().put_to_apex(token, main_id='ImportClassroomId')

    @classmethod
    def _parse_get_response(cls, r: Response) -> 'ApexClassroom':
        kwargs, json_obj = cls._init_kwargs_from_get(r)

        kwargs['program_code'] = course2program_code[int(kwargs['import_org_id'])]
        kwargs['classroom_name'] = json_obj['ClassroomName']
        date = datetime.strptime(kwargs['classroom_start_date'], APEX_DATETIME_FORMAT)
        kwargs['classroom_start_date'] = date.strftime(PS_DATETIME_FORMAT)
        teacher = teacher_fuzzy_match(json_obj['PrimaryTeacher'])
        kwargs['import_user_id'] = teacher.import_user_id

        return cls(**kwargs)

    @classmethod
    def get_all(cls, token, archived=False) -> List['ApexClassroom']:
        """
        Get all objects. Must be overloaded because Apex does not support a global
        GET request for objects in the same. Loops through all PowerSchool objects
        and keeps the ones that exist.

        :param token: Apex access token
        :param bool archived: Whether or not to returned archived results
        :return:
        """
        logger = logging.getLogger(__name__)

        ps_classrooms = fetch_classrooms()
        n_classrooms = len(ps_classrooms)
        logger.info(f'Successfully retrieved {n_classrooms} sections from PowerSchool.')

        ret_val = []

        for i, section in enumerate(map(utils.flatten_ps_json, ps_classrooms)):
            progress = f'section {i}/{n_classrooms}'
            try:
                if not archived and section['RoleStatus'] == 'Archived':
                    continue
            except KeyError:
                logger.debug('JSON object does not contain "RoleStatus"')

            try:
                section_id = section['section_id']
                apex_obj = cls.get(token, section_id)
                ret_val.append(apex_obj)
                logger.info(f'{progress}:Created ApexClassroom for SectionID {section_id}')
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            except exceptions.ApexObjectNotFoundException:
                msg = (f'{progress}:PowerSchool section indexed by {section["section_id"]}'
                       ' could not be found in Apex. Skipping classroom.')
                logger.info(msg)

        logger.info(f'Returning {len(ret_val)} ApexClassroom objects.')
        return ret_val

    def enroll(self, token: str,
               objs: Union[List[ApexDataObject], ApexDataObject]) -> Response:
        """
        Enrolls one or more students or staff members into this classroom.

        :param token: Apex access token
        :param objs: one or more student or staff members; if passed as a list,
                     that list must be homogeneous, i.e. not contain multiple types
        :return: the response to the POST operation
        """
        if issubclass(type(objs), ApexDataObject):
            dtype = type(objs)
            objs = [objs]
        else:
            # Assuming that a non-empty list of objects is passed.
            assert len(objs) > 0
            dtype = type(objs[0])
            # Ensures the list is homogeneous
            assert all(isinstance(x, dtype) for x in objs)

        header = get_header(token)
        url = self._get_data_object_class_url(dtype)

        payload = {dtype.post_heading: []}
        for apex_obj in objs:
            payload_entry = {
                'ImportUserId': apex_obj.import_user_id,
                'ImportOrgId': apex_obj.import_org_id
            }
            payload[dtype.post_heading].append(payload_entry)

        payload = json.dumps(payload)
        return requests.post(url=url, headers=header, data=payload)

    def withdraw(self, token: str, obj: Union['ApexStudent',
                                              'ApexStaffMember']) -> Response:
        """
        Withdraws a single student or staff member from this classroom.

        :param token: Apex access token
        :param obj: the given student or staff member
        :return: the response to the DELETE call
        """
        header = get_header(token)
        url = self._get_data_object_class_url(type(obj))
        return requests.delete(url=url, headers=header)

    def _get_data_object_class_url(self, dtype: Union[Type['ApexStudent'],
                                                      Type['ApexStaffMember']]) -> str:
        """
        Determines the URL path for a GET or DELETE call that enrolls or
        withdraws a student or staff from this class. The result will
        take the form:

        `/objects/{classroomId}/{students/staff}/{importUserId}`

        Choosing from student or staff based on whether the given type
        is `ApexStudent` or `ApexStaffMember`, respectively.

        :param dtype: the relevant data type
        :return: the URL path from enrolling or withdrawing a student from
                 this class
        """
        url = urljoin(self.url + '/', self.import_classroom_id)
        # Get the final component in the object's url
        obj_type_component = urlparse(dtype.url).path.rsplit("/", 1)[-1]
        return urljoin(url + '/', obj_type_component)


def teacher_fuzzy_match(t1: str) -> ApexStaffMember:
    """
    Takes the forename and surname of a teacher in the form of a single string and
    fuzzy matches (case-insensitive) across all teachers from the PowerSchool database.
    Returns the matched teacher as an :class:`ApexStaffMember` object.

    Given that all teachers passed to this helper function should appear in the database,
    any teacher name that differs by more than five characters in length from `t1` is
    passed over unless it is the first one in the list. This concession was made to
    boost performance.

    :param t1: teacher name in the format of "Forename Surname"
    :return: the closest match as measured by Levenshtein distance
    :rtype: ApexStaffMember
    """
    teachers = [ApexStaffMember.from_powerschool(t) for t in fetch_staff()]
    assert len(teachers) > 0

    min_distance = float('inf')
    argmax = 0
    t1 = t1.lower()

    for i, t2 in enumerate(teachers):
        t2_name = t2.first_name + ' ' + t2.last_name
        if abs(len(t1) - len(t2_name)) >= 5 and min_distance != float('inf'):
            # Difference in length of 5 is too large for this context
            continue
        distance = levenshtein_distance(t1, t2_name.lower())
        if distance < min_distance:
            min_distance = distance
            argmax = i

    return teachers[argmax]
