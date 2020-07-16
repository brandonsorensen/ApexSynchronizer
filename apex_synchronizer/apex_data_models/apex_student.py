import logging
import re
import requests
from typing import List, Optional, Union
from urllib.parse import urljoin

from requests import Response

from .utils import BASE_URL, APEX_EMAIL_REGEX, make_userid
from .apex_data_object import ApexDataObject
from .apex_classroom import ApexClassroom
from .. import exceptions
from ..apex_session import TokenType
from ..utils import get_header


class ApexStudent(ApexDataObject):

    """
    Represents a student in the Apex database.

    :param Union[str, int] import_user_id: identifier for the database,
        common to Apex and PowerSchool
    :param Union[str, int] import_org_id: the school to which the
        student belongs
    :param str first_name: the student's first/given name
    :param str middle_name: the student's middle name
    :param str last_name: the student's last/surname
    :param str email: the student's school email address (optional)
    :param int grade_level: the student's grade level
    :param str login_id: the student's login ID
    """

    role = 'S'
    url = urljoin(BASE_URL, 'students')
    post_heading = 'studentUsers'

    ps2apex_field_map = {
        'eduid': 'import_user_id',
        'school_id': 'import_org_id',
        'first_name': 'first_name',
        'middle_name': 'middle_name',
        'last_name': 'last_name',
        'grade_level': 'grade_level',
        'email': 'email'
    }

    def __init__(self, import_user_id: Union[int, str],
                 import_org_id: Union[int, str], first_name: str,
                 middle_name: str, last_name: str, email: str, grade_level: int):
        super().__init__(import_user_id, import_org_id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        if not re.match(APEX_EMAIL_REGEX, email):
            raise exceptions.ApexMalformedEmailException(import_user_id, email)
        self.email = email
        self.grade_level = grade_level
        self.login_id = make_userid(first_name, last_name)
        self.login_pw = import_user_id

    @classmethod
    def _parse_get_response(cls, r: Response):
        kwargs, json_obj = cls._init_kwargs_from_get(r)
        # Just returns the first organization in the list
        # Students should only be assigned to a single org
        kwargs['import_org_id'] = json_obj['Organizations'][0]['ImportOrgId']

        return cls(**kwargs)

    @classmethod
    def from_powerschool(cls, json_obj: dict) -> 'ApexStudent':
        try:
            kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        except KeyError as e:
            if e.args[0].lower() == 'email':
                try:
                    eduid = json_obj['tables']['students']['eduid']
                except KeyError:
                    raise exceptions.ApexMalformedJsonException(json_obj)

                raise exceptions.ApexNoEmailException(eduid)
            raise e

        if kwargs['import_user_id'] is None:
            kwargs['import_user_id'] = '10'

        return cls(**kwargs)

    def get_enrollments(self, token: TokenType) -> Optional[List['ApexClassroom']]:
        """
        Gets all classes in which this :class:`ApexStudent` is enrolled.

        :param token: an Apex access token
        :return: a list of ApexClassroom objects
        """
        classroom_ids = self.get_enrollment_ids(token)
        ret_val = []
        n_classrooms = len(classroom_ids)
        logger = logging.getLogger(__name__)

        for i, c_id in enumerate(classroom_ids):
            progress = f'Classroom {i + 1}/{n_classrooms}:id {c_id}:'
            logger.info(f'{progress}:retrieving classroom info from Apex.')
            try:
                ret_val.append(ApexClassroom.get(token, str(c_id)))
            except exceptions.ApexObjectNotFoundException:
                logger.info(f'Could not retrieve classroom {c_id}. Skipping..')
            except exceptions.ApexConnectionException:
                logger.exception('Could not connect to Apex endpoint.')
                return

        return ret_val

    def get_enrollment_ids(self, token: TokenType) -> List[int]:
        """
        Gets the `ImportClassroomId` of all objects in which the student
        is enrolled. Differs from the `get_enrollments` in that it only
        returns the IDs instead of `ApexClassroom` objects. This makes
        it a great deal faster because only a single call to Apex is
        made.

        :param token: an Apex access token
        :return: a list of IDs for each classroom in which the student
            is enrolled
        :rtype: List[int]
        """
        header = get_header(token)
        r = requests.get(url=self.classroom_url, headers=header,
                         params={'isActiveOnly': True})
        try:
            r.raise_for_status()
            return [int(student['ImportClassroomId']) for student in r.json()]
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException(self.import_user_id)
        except KeyError:
            raise exceptions.ApexMalformedJsonException(r.json())

    def transfer(self, token: TokenType, old_classroom_id: str,
                 new_classroom_id: str, new_org_id: str = None) -> Response:
        """
        Transfers student along with role and grade data from one
        classroom to another

        :param token: Apex access token
        :param old_classroom_id: id of current classroom
        :param new_classroom_id: id of the classroom to which the
            student will be transferred
        :param new_org_id: optional new org_id
        :return: the response to the PUT operation
        """
        header = get_header(token)
        url = urljoin(self.classroom_url + '/', old_classroom_id)
        params = {'newClassroomID': new_classroom_id}
        if new_org_id is not None:
            params['toOrgId'] = new_org_id

        r = requests.put(url=url, headers=header, params=params)
        return r

    def enroll(self, token: TokenType, classroom_id: str) -> Response:
        """
        Enrolls this :class:`ApexStudent` object into the class indexed
        by `classroom_id`.

        :param token: an Apex access token
        :param classroom_id: the ID of the relevant classroom
        :return: the response of the PUT call
        """
        classroom = ApexClassroom.get(token, classroom_id)
        return classroom.enroll(token, self)

    def withdraw(self, token: str, classroom_id: str) -> Response:
        classroom = ApexClassroom.get(token, classroom_id)
        return classroom.withdraw(token, self.import_user_id)

    @property
    def classroom_url(self) -> str:
        url = urljoin(self.url + '/', self.import_user_id)
        url = urljoin(url + '/', 'classrooms')
        return url
