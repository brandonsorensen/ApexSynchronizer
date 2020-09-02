from datetime import datetime
from requests import Response
from typing import Collection, Dict, List, Optional, Sequence, Union
from urllib.parse import urljoin, urlparse
import json
import logging

import requests

from . import utils as adm_utils
from .apex_data_object import ApexDataObject, ApexNumericId, ApexUser
from .apex_staff_member import ApexStaffMember
from .page_walker import PageWalker
from .utils import check_args
from .. import exceptions, utils
from ..apex_session import TokenType
from ..ps_agent import course2program_code, fetch_staff, fetch_classrooms
from ..utils import get_header, levenshtein_distance
import apex_synchronizer.apex_data_models as adm


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


class ApexClassroom(ApexNumericId, ApexDataObject):

    """
    Represents a classroom in the Apex data base.

    :param Union[int, str] import_classroom_id:
                            identifier for the database, common to
                            Apex and PowerSchool
    :param Union[int, str] import_org_id: the school to which teacher
                                          staff member belongs
    :param Union[int, str] import_user_id: teacher's ImportId
    :param str classroom_name: name of the classroom
    :param List[str] product_codes: the Apex product codes representing
        which curricula are taught in the classroom
    :param datetime classroom_start_date: the day the classroom starts
    :param str program_code: the program to which the classroom belongs
    """

    url = urljoin(adm_utils.BASE_URL, 'classrooms')
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

    def __init__(self, import_org_id: int, import_classroom_id: int,
                 classroom_name: str, product_codes: [str],
                 import_user_id: str,
                 classroom_start_date: str, program_code: str):
        super().__init__(import_user_id=import_user_id.lower().strip(),
                         import_org_id=import_org_id)
        try:
            self.import_classroom_id = int(import_classroom_id)
        except TypeError:
            if import_classroom_id is None:
                raise exceptions.NoUserIdException()
            raise exceptions.InvalidIDException(import_classroom_id,
                                                self.__class__)
        if self.import_classroom_id < 0:
            raise exceptions.InvalidIDException(import_classroom_id,
                                                self.__class__)
        self.classroom_name = classroom_name
        self.product_codes = product_codes
        if not self.product_codes:
            raise exceptions.NoProductCodesException(self.import_classroom_id)
        try:
            self.classroom_start_date = datetime.strptime(
                classroom_start_date, adm_utils.PS_DATETIME_FORMAT
            )
        except ValueError:
            raise exceptions.ApexDatetimeException(classroom_start_date)
        self.program_code = program_code

    def change_teacher(self, new_teacher: Union['ApexStaffMember', str],
                       token: TokenType = None,
                       session: requests.Session = None):
        """
        Withdraws the current teachers and enrolls a new one as a
        primary teacher.

        :param new_teacher: the new teacher
        :param token: an Apex access token
        :param session: an existing Apex session
        """
        logger = logging.getLogger(__name__)
        if type(new_teacher) is str:
            new_teacher = ApexStaffMember.get(new_teacher, token=token,
                                              session=session)
        logger.info(f'Changing teachers from {self.import_user_id} '
                    f'to {new_teacher.import_user_id}.')
        current_teacher = ApexStaffMember.get(self.import_user_id,
                                              token=token, session=session)

        r = self.withdraw(current_teacher, token=token, session=session)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            return

        self.enroll(new_teacher, token=token, session=session)

    def enroll(self, objs: Union[Sequence[ApexUser], ApexUser],
               token: TokenType = None,
               session: requests.Session = None) -> Response:
        """
        Enrolls one or more students or staff members into this
        classroom.

        :param token: Apex access token
        :param session: an existing Apex session
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

        if not isinstance(session, requests.Session):
            header = get_header(token)
        else:
            header = None
        url = self._get_data_object_class_url(dtype)

        logger = logging.getLogger(__name__)
        payload = {dtype.post_heading: []}
        logger.info(f'Creating payload for {len(objs)} objects.')
        for apex_obj in objs:
            logger.debug('Creating payload for obj with ID '
                         f'\"{apex_obj.import_user_id}\".')
            payload_entry = {
                'ImportUserId': str(apex_obj.import_user_id),
                'ImportOrgId': str(apex_obj.import_org_id)
            }
            if isinstance(apex_obj, ApexStaffMember):
                payload_entry['IsPrimary'] = True
            payload[dtype.post_heading].append(payload_entry)

        payload = json.dumps(payload)
        logger.info('Posting payload.')
        r = agent.post(url=url, headers=header, data=payload)
        try:
            r.raise_for_status()
            logger.debug('Received response ' + str(r.status_code))
        except requests.HTTPError:
            logger.debug('Enrollment failed with the following response:\n'
                         + r.text)
        return r

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
        url = urljoin(self.url + '/', str(self.import_classroom_id))
        url = urljoin(url + '/', 'reports')
        if isinstance(agent, requests.Session):
            r = agent.get(url=url)
        else:
            r = agent.get(url=url, headers=get_header(token))

        try:
            r.raise_for_status()
            if r.status_code == 204:
                return [{}]
            return r.json()
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException(
                self.import_classroom_id
            )

    def to_json(self) -> dict:
        d = super().to_json()
        dt_obj = self.classroom_start_date
        d['ClassroomStartDate'] = dt_obj.strftime(adm_utils.PS_DATETIME_FORMAT)
        return d

    def update(self, new_classroom: 'ApexClassroom',
               token: TokenType = None,
               session: requests.Session = None) -> Optional[requests.Response]:
        """

        :param new_classroom:
        :param token:
        :param session:
        :return:
        """
        if self == new_classroom:
            return

        if self.import_classroom_id != new_classroom.import_classroom_id:
            raise ValueError('`new_classroom` object contains a different'
                             'classroom ID.')

        if self.import_user_id != new_classroom.import_user_id:
            self.change_teacher(new_classroom.import_user_id, token=token,
                                session=session)

        return new_classroom.put_to_apex(token=token, session=session)

    def update_product_codes(self, new_codes: Union[List[str], str],
                             token: TokenType = None,
                             session: requests.Session = None) \
            -> requests.Response:
        """
        Updates the product codes and submits the change to the Apex
        API, returning the response from the PUT call.

        :param new_codes: the new product codes to submit to Apex
        :param token: an Apex access token
        :param session: an existing Apex session
        :raises exceptions.ApexNoChangeSubmitted: when the codes are
            the same
        :raises ValueError: when new_codes is empty or does not contain
            only strings
        :return: the responses returned by the PUT and/or DELETE calls.
        """

        if isinstance(new_codes, str):
            new_codes = [new_codes]
        else:
            if not new_codes:
                raise ValueError('No new codes provided.')
            if not all(isinstance(code, str) for code in new_codes):
                raise ValueError('Argument "new_codes" contains an object that '
                                 'is not of type `str`.')
            new_codes = set(new_codes)
        logger = logging.getLogger(__name__)
        old_codes = set(self.product_codes)
        if new_codes == old_codes:
            logger.debug('Product codes will not be updated because the '
                         'provided codes are the same as the current ones.')
            raise exceptions.ApexNoChangeSubmitted()

        self.product_codes = new_codes
        r = self.put_to_apex(token=token, session=session)
        return r

    def withdraw(self, obj: Union[ApexUser, int, str],
                 obj_type: str = None,
                 token: TokenType = None,
                 session: requests.Session = None) -> Response:
        """
        Withdraws a single student or staff member from this classroom.

        :param token: Apex access token
        :param session: an existing Apex session
        :param obj_type: one of either 'S' for student or 'T' for
            teacher/staff member
        :param obj: the given student or staff member
        :return: the response to the DELETE call
        """
        logger = logging.getLogger(__name__)
        agent = check_args(token, session)
        if obj_type is not None:
            if obj_type.lower() not in ('s', 't'):
                raise ValueError("`obj_type` must be one of ('S', 'T'), "
                                 "case insensitive.")
            dtype = (adm.ApexStudent if obj_type.lower() == 'S'
                     else ApexStaffMember)
        else:
            if isinstance(obj, ApexUser):
                user_id = obj.import_user_id
                dtype = type(obj)
            else:
                raise ValueError('Must supply argument to `obj_type` or '
                                 'pass an object of a type that subclasses'
                                 'the ApexUser class.')
        try:
            user_id
        except NameError:
            user_id = obj

        url = self._get_data_object_class_url(dtype)
        url = urljoin(url + '/', str(user_id))
        logger.info('Withdrawing user from classroom.')
        r = agent.delete(url=url)
        try:
            r.raise_for_status()
            logger.debug('Successfully withdrawn')
        except requests.HTTPError:
            logger.debug('Failed to withdraw; received response\n'
                         + r.text)
        return r

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
        ps_dt = datetime.strptime(kwargs['classroom_start_date'],
                                  adm_utils.PS_OUTPUT_FORMAT)
        apex_dt = ps_dt.strftime(adm_utils.PS_DATETIME_FORMAT)
        kwargs['classroom_start_date'] = apex_dt

        return cls(**kwargs)

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
        url = urljoin(self.url + '/', str(self.import_classroom_id))
        # Get the final component in the object's url
        obj_type_component = urlparse(dtype.url).path.rsplit("/", 1)[-1]
        return urljoin(url + '/', obj_type_component)

    def _get_put_payload(self) -> dict:
        payload = super()._get_put_payload()
        if 'ProgramCode' in payload.keys():
            del payload['ProgramCode']
        if 'Role' in payload.keys():
            del payload['Role']
        payload['IsPrimary'] = True
        return payload

    @classmethod
    def _get_all(cls, token: TokenType, ids_only: bool, archived: bool,
                 session: requests.Session) -> List[Union['ApexClassroom', int]]:
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
                logger.info(f'{progress}:Created ApexClassroom for '
                            f'SectionID {section_id}')
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            except exceptions.ApexIncompleteDataException as e:
                logger.info(f'{progress}:{e}')
            except exceptions.ApexObjectNotFoundException:
                msg = (f'{progress}:PowerSchool section indexed by '
                       f'{section["section_id"]} could not be found in Apex. '
                       'Skipping classroom.')
                logger.info(msg)

        logger.info(f'Returning {len(ret_val)} ApexClassroom objects.')
        return ret_val

    @classmethod
    def _parse_get_response(cls, r: Response) -> 'ApexClassroom':
        kwargs, json_obj = cls._init_kwargs_from_get(r)

        org_id = int(kwargs['import_org_id'])
        kwargs['program_code'] = course2program_code[org_id]
        kwargs['classroom_name'] = json_obj['ClassroomName']
        date = datetime.strptime(kwargs['classroom_start_date'],
                                 adm_utils.APEX_DATETIME_FORMAT)
        kwargs['classroom_start_date'] = date.strftime(
            adm_utils.PS_DATETIME_FORMAT
        )
        if not json_obj['PrimaryTeacher']:
            raise exceptions.ApexNoTeacherException(json_obj)
        teacher = teacher_fuzzy_match(json_obj['PrimaryTeacher'],
                                      org=org_id,
                                      teachers=cls._all_ps_teachers)
        kwargs['import_user_id'] = teacher.import_user_id

        return cls(**kwargs)


def get_classrooms_for_eduids(eduids: Collection[int], token: TokenType = None,
                              session: requests.Session = None,
                              ids_only: bool = False,
                              return_empty: bool = False) \
        -> Dict[int, List[Union[int, ApexClassroom]]]:
    logger = logging.getLogger(__name__)
    base_url = urljoin(adm_utils.BASE_URL, '/students/')

    def url_for_eduid(eduid_: int) -> str:
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
        except exceptions.ApexNoEnrollmentsError as e:
            logger.info(str(e))
            eduid_classrooms = []
        except exceptions.ApexError:
            logger.exception('Received generic Apex error:\n')
            eduid_classrooms = []

        if eduid_classrooms or (not eduid_classrooms and return_empty):
            classrooms[int(eduid)] = eduid_classrooms

    return classrooms


def handle_400_response(r: Response, logger: logging.Logger = None):
    if logger is None:
        logger = logging.getLogger(__name__)

    errors = _parse_400_response(r, logger)
    return errors


def teacher_fuzzy_match(t1: str, org: Union[str, int] = None,
                        teachers: Collection[ApexStaffMember] = None) \
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
    :param org: the school to which the teacher belongs, to make search
        quicker and more accurate, optional
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
    org_id = int(org) if org is not None else None

    for i, t2 in enumerate(teachers):
        t2_name = t2.first_last
        t2_org = int(t2.import_org_id)
        if org_id is not None and org_id != t2_org:
            continue
        if abs(len(t1) - len(t2_name)) >= 5 and min_distance != float('inf'):
            # Difference in length of 5 is too large for this context
            continue
        distance = levenshtein_distance(t1, t2_name.lower())
        if distance < min_distance:
            min_distance = distance
            argmax = i

    return teachers[argmax]


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
            if page_response.status_code == 401:
                raise exceptions.ApexAuthenticationError()
            try:
                message = page_response.json()['message']
            except KeyError as ke:
                logger.debug(f'Received unknown response:\n{page_response.json()}')
                raise exceptions.ApexMalformedJsonException(page_response) \
                    from ke

            if message == 'Results not found.':
                eduid = url.split('/')[-2]
                raise exceptions.ApexNoEnrollmentsError(eduid)
            elif message == "User doesn't exist.":
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


def _parse_400_response(r: Response, logger: logging.Logger = None) \
        -> Optional[Dict[int, adm_utils.PostErrors]]:
    """
    Parses a 400 response and takes actions based on the possible
    errors.

    :param r: the response to parse
    :param logger: an optional logger, defaults to the
        logger of the `apex_classroom` module
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if r.status_code == 401:
        raise exceptions.ApexAuthenticationError()

    as_json = r.json()

    if type(as_json) is list:
        # TODO
        logger.debug('Received response: ' + str(r))
        return
    elif type(as_json) is not dict:
        raise exceptions.ApexError()

    try:
        errors = as_json['classroomEntries']
    except KeyError as ke:
        raise exceptions.ApexError from ke

    ret_val = {}
    e: dict
    for e in errors:
        classroom_id = int(e['ImportClassroomId'])
        msg = e['Message']
        ret_val[classroom_id] = adm_utils.PostErrors.get_for_message(msg)

    return ret_val
