from datetime import datetime
from enum import Enum
from requests import Response
from typing import Collection, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse
import json
import logging

import requests

from .apex_data_object import ApexDataObject, ApexUser
from .apex_staff_member import ApexStaffMember
from .page_walker import PageWalker
from .utils import (BASE_URL, APEX_DATETIME_FORMAT,
                    PS_DATETIME_FORMAT, check_args)
from .. import exceptions, utils
from ..apex_session import TokenType
from ..ps_agent import course2program_code, fetch_staff, fetch_classrooms
from ..utils import get_header, levenshtein_distance


def _init_powerschool_teachers() -> List[ApexStaffMember]:
    """Creates `ApexStaffMember` objects from all PowerSchool teachers."""
    logger = logging.getLogger(__name__)
    teachers = []
    for t in fetch_staff():
        try:
            teachers.append(ApexStaffMember.from_powerschool(t))
        except exceptions.ApexDataObjectException:
            logger.debug('Could not create teacher from the following JSON:\n'
                         + str(t))

    assert len(teachers) > 0
    return teachers


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
    main_id = 'ImportClassroomId'
    _all_ps_teachers = _init_powerschool_teachers()

    def __init__(self, import_org_id: Union[str, int],
                 import_classroom_id: Union[str, int],
                 classroom_name: str, product_codes: [str],
                 import_user_id: str,
                 classroom_start_date: str, program_code: str):
        super().__init__(import_user_id, import_org_id)
        self.import_classroom_id = import_classroom_id
        self.classroom_name = classroom_name
        self.product_codes = product_codes
        if not self.product_codes:
            raise exceptions.NoProductCodesException(self.import_classroom_id)
        self.classroom_start_date = classroom_start_date
        self.program_code = program_code

    @classmethod
    def from_powerschool(cls, json_obj: dict, already_flat: bool = False) \
            -> 'ApexClassroom':
        kwargs = cls._init_kwargs_from_ps(json_obj, already_flat)
        kwargs['classroom_name'] = (kwargs['course_name']
                                    + ' - '
                                    + kwargs['section_number'])
        del kwargs['course_name']
        del kwargs['section_number']

        kwargs['program_code'] = course2program_code[int(kwargs['import_org_id'])]
        if kwargs['product_codes'] is None:
            product_codes = []
        else:
            product_codes = [kwargs['product_codes']]

        kwargs['product_codes'] = product_codes

        return cls(**kwargs)

    @classmethod
    def _parse_get_response(cls, r: Response) -> 'ApexClassroom':
        kwargs, json_obj = cls._init_kwargs_from_get(r)

        kwargs['program_code'] = course2program_code[int(kwargs['import_org_id'])]
        kwargs['classroom_name'] = json_obj['ClassroomName']
        date = datetime.strptime(kwargs['classroom_start_date'],
                                 APEX_DATETIME_FORMAT)
        kwargs['classroom_start_date'] = date.strftime(PS_DATETIME_FORMAT)
        teacher = teacher_fuzzy_match(json_obj['PrimaryTeacher'],
                                      cls._all_ps_teachers)
        kwargs['import_user_id'] = teacher.import_user_id

        return cls(**kwargs)

    @classmethod
    def get_all(cls, token: TokenType, ids_only: bool = False,
                archived: bool = False,
                session: requests.Session = None) -> List[Union['ApexClassroom', int]]:
        """
        Get all objects. Must be overloaded because Apex does not
        support a global GET request for objects in the same. Loops
        through all PowerSchool objects and keeps the ones that exist.

        Note: Because the Apex API does not provide a GET operation
        that returns all classrooms, fetching only IDs does not yield
        any performance boost. All classrooms must stll be fetched
        individually.

        :param token: Apex access token
        :param session: an existing requests.Session object
        :param bool ids_only: Whether or not to return only IDs
        :param bool archived: Whether or not to returned archived
            results
        :return:
        """
        logger = logging.getLogger(__name__)

        ret_val = []

        for i, (section, progress) in enumerate(walk_ps_sections(archived)):
            try:
                section_id = section['section_id']
                apex_obj = cls.get(section_id, token=token, session=session)
                ret_val.append(int(apex_obj.import_classroom_id) if ids_only
                               else apex_obj)
                logger.info(f'{progress}:Created ApexClassroom for'
                            f'SectionID {section_id}')
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            except exceptions.ApexObjectNotFoundException:
                msg = (f'{progress}:PowerSchool section indexed by '
                       f'{section["section_id"]} could not be found in Apex. '
                       'Skipping classroom.')
                logger.info(msg)

        logger.info(f'Returning {len(ret_val)} ApexClassroom objects.')
        return ret_val

    def enroll(self, objs: Union[List[ApexDataObject], ApexDataObject],
               token: TokenType = None,
               session: requests.Session = None) -> Response:
        """
        Enrolls one or more students or staff members into this
        classroom.

        :param token: Apex access token
        :param objs: one or more student or staff members; if passed as
            a list, that list must be homogeneous, i.e. not contain
            multiple types
        :return: the response to the POST operation
        """
        agent = check_args(token=token, session=session)
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
        return agent.post(url=url, headers=header, data=payload)

    def withdraw(self, token: TokenType, obj: ApexUser) -> Response:
        """
        Withdraws a single student or staff member from this classroom.

        :param token: Apex access token
        :param obj: the given student or staff member
        :return: the response to the DELETE call
        """
        header = get_header(token)
        url = self._get_data_object_class_url(type(obj))
        return requests.delete(url=url, headers=header)

    def get_reports(self, token: TokenType = None,
                    session: requests.Session = None) -> List[dict]:
        """
        Returns summary-level gradebook information for each student
        that is enrolled in the classroom. If a 200 response is returned
        the JSON is returned from this method as is without any
        validation. The documentation on what can be expected in this
        JSON object can be found on Apex's website.

        Either an access token or an existing session `may` be passed,
        but at least one `must` be passed. If both a token and a session
        are given, the session takes priority.

        :param token: an Apex access token
        :param session: an existing Apex session
        :return: gradebook info for each student enrolled in the
            classroom
        """
        agent = check_args(token, session)
        url = urljoin(self.url + '/', self.import_classroom_id)
        url = urljoin(url + '/', 'reportss')
        if isinstance(agent, requests.Session):
            r = agent.get(url=url)
        else:
            r = agent.get(url=url, headers=get_header(token))

        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException

        return r.json()

    def _get_data_object_class_url(self, dtype: ApexUser) -> str:
        """
        Determines the URL path for a GET or DELETE call that enrolls or
        withdraws a student or staff from this class. The result will
        take the form:

        `/objects/{classroomId}/{students/staff}/{importUserId}`

        Choosing from student or staff based on whether the given type
        is `ApexStudent` or `ApexStaffMember`, respectively.

        :param dtype: the relevant data type
        :return: the URL path from enrolling or withdrawing a student
                 from this class
        """
        url = urljoin(self.url + '/', self.import_classroom_id)
        # Get the final component in the object's url
        obj_type_component = urlparse(dtype.url).path.rsplit("/", 1)[-1]
        return urljoin(url + '/', obj_type_component)


def teacher_fuzzy_match(t1: str, teachers: Collection[ApexStaffMember] = None) \
        -> ApexStaffMember:
    """
    Takes the forename and surname of a teacher in the form of a single
    string and fuzzy matches (case-insensitive) across all teachers
    from the PowerSchool database. Returns the matched teacher as an
    :class:`ApexStaffMember` object.

    Given that all teachers passed to this helper function should
    appear in the database, any teacher name that differs by more than
    five characters in length from `t1` is passed over unless it is the
    first one in the list. This concession was made to boost
    performance.

    :param t1: teacher name in the format of "Forename Surname"
    :param teachers: an optional list of teachers as ApexStaffMember
        objs
    :return: the closest match as measured by Levenshtein distance
    :rtype: ApexStaffMember
    """
    logger = logging.getLogger(__name__)
    logger.debug('Fuzzy matching teacher: ' + str(t1))

    if teachers is None:
        logger.debug('Fetching teachers.')
        teachers = []
        for t in fetch_staff():
            try:
                teachers.append(ApexStaffMember.from_powerschool(t))
                logger.debug('Successfully fetched and created ' + str(t))
            except exceptions.ApexEmailException as e:
                logger.debug('Failed to create ' + str(t))
                logger.debug(e)
        assert len(teachers) > 0
        logger.debug(f'Successfully created {len(teachers)} teachers.')

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


def get_classrooms_for_eduids(eduids: Collection[Union[str, int]],
                              token: TokenType = None,
                              session: requests.Session = None,
                              ids_only: bool = False,
                              return_empty: bool = False) \
        -> Dict[int, List[Union[int, ApexClassroom]]]:
    logger = logging.getLogger(__name__)
    base_url = urljoin(BASE_URL, '/students/')

    def url_for_eduid(eduid_: Union[str, int]) -> str:
        url_ = urljoin(base_url, str(eduid_))
        return urljoin(url_ + '/', 'classrooms')

    classrooms = {}
    logger.info(f'Getting classrooms for {len(eduids)} students.')
    for i, eduid in enumerate(eduids):
        logger.info(f'{i + 1}/{len(eduids)} students')
        url = url_for_eduid(eduid)
        try:
            eduid_classrooms = _get_classroom_for_eduid(url, token=token,
                                                        session=session,
                                                        ids_only=ids_only)
        except exceptions.ApexObjectNotFoundException:
            logger.info('Could not find student or student is not enrolled in '
                        'any Apex classes: ' + str(eduid))
            eduid_classrooms = []
        except exceptions.ApexError:
            logger.exception('Received generic Apex error:\n')
            eduid_classrooms = []

        if eduid_classrooms or (not eduid_classrooms and return_empty):
            classrooms[int(eduid)] = eduid_classrooms

    return classrooms


def _get_classroom_for_eduid(url: str, token: TokenType = None,
                             session: requests.Session = None,
                             ids_only: bool = False) \
        -> List[Union[int, ApexClassroom]]:
    check_args(token, session)
    logger = logging.getLogger(__name__)
    ret_val = []
    custom_args = {'isActiveOnly': 'true'}
    walker = PageWalker(logger=logger, session=session)

    for i, page_response in enumerate(walker.walk(url,
                                                  custom_args=custom_args)):
        try:
            page_response.raise_for_status()
        except requests.exceptions.HTTPError:
            try:
                message = page_response.json()['message']
            except KeyError:
                logger.debug(f'Received unknown response:\n{page_response.json()}')
                raise requests.exceptions.RequestException()

            if (message == 'Results not found.'
                    or message == "User doesn't exist."):
                eduid = url.split('/')[-2]
                raise exceptions.ApexObjectNotFoundException(eduid)
            raise exceptions.ApexError()
        except requests.exceptions.ConnectionError:
            raise exceptions.ApexConnectionException()
        except StopIteration:
            return ret_val

        json_obj = page_response.json()
        ApexClassroom._parse_response_page(token=token, session=session,
                                           json_objs=json_obj, page_number=i,
                                           all_objs=ret_val, archived=False,
                                           ids_only=ids_only)

    return ret_val


def walk_ps_sections(archived: bool):
    logger = logging.getLogger(__name__)

    ps_classrooms = fetch_classrooms()
    n_classrooms = len(ps_classrooms)
    logger.info(f'Successfully retrieved {n_classrooms} sections'
                'from PowerSchool.')

    for i, section in enumerate(map(utils.flatten_ps_json, ps_classrooms)):
        try:
            if not archived and section['RoleStatus'] == 'Archived':
                continue
        except KeyError:
            logger.debug('JSON object does not contain "RoleStatus"')

        progress = f'section {i + 1}/{n_classrooms}'
        yield section, progress


class ClassroomPostErrors(Enum):
    NotAvailableOrder = 1
    UserDoesNotExist = 2
    Unrecognized = 3


post_error_map = {
    "User doesn't exist": ClassroomPostErrors.UserDoesNotExist,
    'No available Order': ClassroomPostErrors.NotAvailableOrder,
}


def handle_400_response(r: Response, logger: logging.Logger = None):
    if logger is None:
        logger = logging.getLogger(__name__)

    errors = _parse_400_response(r, logger)
    print(errors)
    return errors


def _parse_400_response(r: Response, logger: logging.Logger = None) \
        -> Optional[Dict[int, ClassroomPostErrors]]:
    """
    Parses a 400 response and takes actions based on the possible
    errors.

    :param r: the response to parse
    :param logger: an optional logger, defaults to the
        logger of the `apex_classroom` module
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    as_json = r.json()

    if type(as_json) is list:
        # TODO
        logger.debug('Received response: ' + str(r))
        return
    elif type(as_json) is not dict:
        raise exceptions.ApexError()

    try:
        errors = as_json['classroomEntries']
    except KeyError:
        raise exceptions.ApexError

    ret_val = {}
    e: dict
    for e in errors:
        classroom_id = int(e['ImportClassroomId'])
        for msg, post_error in post_error_map.items():
            if e['Message'].startswith(msg):
                ret_val[classroom_id] = post_error

        if classroom_id not in ret_val.keys():
            ret_val[classroom_id] = ClassroomPostErrors.Unrecognized

    return ret_val
