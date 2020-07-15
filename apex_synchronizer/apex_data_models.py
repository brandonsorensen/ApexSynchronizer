import logging
import json
import requests
import re
from . import utils, exceptions
from abc import ABC, abstractmethod
from datetime import datetime
from .ps_agent import course2program_code, fetch_staff, fetch_classrooms
from string import punctuation
from typing import Collection, List, Optional, Tuple, Type, Union
from requests.models import Response
from urllib.parse import urljoin, urlparse
from .utils import BASE_URL, get_header

APEX_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
PS_DATETIME_FORMAT = '%Y/%m/%d'
PUNC_REGEX = re.compile(fr'[{punctuation + " "}]')
APEX_EMAIL_REGEX = re.compile("^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}"
                              "[a-zA-Z0-9])?)+$|^$/]")


class ApexDataObject(ABC):

    """
    The base class from which `ApexStaffMember`, 'ApexStudent` and `ApexClassroom`
    will inherit. Defines a number of class methods common to all objects that
    aid in making RESTful calls to the Apex API. Additionally, contains a number
    of abstract methods that must be implemented by the subclasses.
    """

    def __init__(self, import_user_id, import_org_id):
        """Initializes instance variables."""
        self.import_user_id = import_user_id
        if not import_user_id:
            raise exceptions.NoUserIdException
        self.import_org_id = import_org_id

    @classmethod
    def get(cls, token, import_id: Union[str, int]) -> 'ApexDataObject':
        """
        Gets the ApexDataObject corresponding to a given ImportId.

        :param token: ApexAccessToken
        :param import_id: the ImportId of the object
        :return: an ApexDataObject corresponding to the given ImportId
        """
        try:
            r = cls._get_response(token, str(import_id))
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException(import_id)
        except requests.exceptions.ConnectionError:
            raise exceptions.ApexConnectionException()

        return cls._parse_get_response(r)

    @classmethod
    @abstractmethod
    def _parse_get_response(cls, r: Response) -> 'ApexDataObject':
        """
        A helper method for the `get` method. Parses the JSON object returned by
        the `_get_response`, validates it, and returns an instance of
        the corresponding class.

        :param Response r: the reponse returned by the `_get_response` method.
        :return: an instance of type `cls` corresponding to the JSON object in `r`.
        """
        pass

    @classmethod
    def _get_response(cls, token: str, import_id) -> Response:
        """
        Calls a GET operation for a given ImportId and returns the response. The
        first (and constant across all subclasses) component of the `get` method.

        :param token: the Apex access token
        :param import_id:
        :return: the response from the GET operation
        """
        custom_args = {
            'importUserId': import_id
        }
        header = get_header(token, custom_args)
        url = urljoin(cls.url + '/', import_id)
        r = requests.get(url=url, headers=header)
        return r

    @classmethod
    def get_all(cls, token, ids_only=False, archived=False)\
            -> List[Union['ApexDataObject', int]]:
        """
        Gets all objects of type `cls` in the Apex database.

        :param token: Apex access token
        :param archived: whether or not to return archived objects
        :return: a list containing all objects of this type in the Apex database
        """
        logger = logging.getLogger(__name__)

        current_page = 1
        ret_val = []

        header = get_header(token)
        r = requests.get(url=cls.url, headers=header)
        total_pages = int(r.headers['total-pages'])
        while current_page <= total_pages:
            logger.info(f'Reading page {current_page}/{total_pages} of get_all response.')
            cls._parse_response_page(token=token, json_objs=r.json(), page_number=current_page,
                                     all_objs=ret_val, archived=archived,
                                     ids_only=ids_only)
            current_page += 1
            header['page'] = str(current_page)

            if current_page <= total_pages:
                r = requests.get(url=cls.url, headers=header)

        return ret_val

    def post_to_apex(self, token) -> Response:
        """
        Posts the information contained in this object to the Apex API. Simply
        a convenience method that passes this object to the `post_batch` class
        method.

        :param token: Apex access token
        :return: the response returned by the POST operation
        """
        return self.post_batch(token, [self])

    @classmethod
    def post_batch(cls, token: str, objects: Collection['ApexDataObject']):
        """
        Posts a batch of `ApexDataObjects` to the Apex API. The `object` parameter
        must be heterogeneous, i.e. must contain objects of all the same type.
        Attempting to post an object not of the correct subclass (i.e., attempting
        to call `ApexStudent.post_batch` with even one `ApexStaffMember` will
        result in an error.

        :param token: Apex access token
        :param objects: a heterogeneous collection of `ApexDataObjects`
        :return: the result of the POST operation
        """
        header = get_header(token)
        payload = json.dumps({cls.post_heading: [c.to_json() for c in objects]})
        url = cls.url if len(objects) <= 50 else urljoin(cls.url + '/', 'batch')
        r = requests.post(url=url, data=payload, headers=header)
        return r

    def delete_from_apex(self, token) -> Response:
        """
        Deletes this object from the Apex database

        :param token: Apex access token
        :return: the response from the DELETE operation
        """
        custom_args = {
            'importUserId': self.import_user_id,
            'orgId': self.import_org_id
        }
        header = get_header(token, custom_args)
        url = urljoin(self.url + '/', self.import_user_id)
        r = requests.delete(url=url, headers=header)
        return r

    def put_to_apex(self, token, main_id='ImportUserId') -> Response:
        """
        Useful for updating a record in the Apex database.

        :param token: Apex access token
        :param main_id: the idenitifying class attribute: ImportUserId for
                        `ApexStudent` and `ApexStaffMember` objects,
                        ImportClassroomId for `ApexClassroom` objects
        :return: the response from the PUT operation.
        """
        header = get_header(token)
        url = urljoin(self.url + '/', self.import_user_id)
        payload = self.to_json()
        del payload[main_id]  # Given in the URL
        # We don't want to update a password
        if 'LoginPw' in payload.keys():
            del payload['LoginPw']
        r = requests.put(url=url, headers=header, data=payload)
        return r

    @property
    @abstractmethod
    def url(self) -> str:
        """The class's base URL."""
        pass

    @property
    @abstractmethod
    def post_heading(self) -> str:
        """The heading required for a POST call."""
        pass

    @property
    @abstractmethod
    def role(self) -> str:
        """The role of a given class, either T or S"""
        pass

    @property
    @abstractmethod
    def ps2apex_field_map(self) -> dict:
        """
        A mapping from field names return by PowerSchool queries to their respective
        Apex JSON fields for each class.
        """
        pass

    @classmethod
    @abstractmethod
    def from_powerschool(cls, json_obj: dict) -> 'ApexDataObject':
        """
        Creates an instance of the class from a JSON object returned from PowerSchool.

        :param json_obj: the PowerSchool JSON object
        :return: an instance of type cls representing the JSON object
        """
        pass

    @classmethod
    def _init_kwargs_from_ps(cls, json_obj):
        """
        A helper method for the `from_powerschool` method. Takes the PowerSchool JSON and
        transforms it according to `ps2apex_field_map` mappings.

        :param json_obj: the PowerSchool JSON object
        :return: the same JSON object with transformed keys.
        """
        kwargs = {}
        json_obj = utils.flatten_ps_json(json_obj)
        for ps_key, apex_key in cls.ps2apex_field_map.items():
            if type(apex_key) is str:
                kwargs[apex_key] = json_obj[ps_key]
            else:
                for k in apex_key:
                    kwargs[k] = json_obj[ps_key]
        return kwargs

    @classmethod
    def _init_kwargs_from_get(cls, r: Response) -> Tuple[dict, dict]:
        """
        Helper method for the `get` method. Converts the keys from a GET
        response JSON object into the proper style for initializing
        ApexDataObject objects.

        :param r: the response of a GET call
        :return: a Tuple of the converted mappings and the original JSON
            response
        """
        json_obj = json.loads(r.text)
        kwargs = {}
        params = set(cls.ps2apex_field_map.values())
        for key, value in json_obj.items():
            snake_key = utils.camel_to_snake(key)
            if snake_key in params:
                kwargs[snake_key] = value

        return kwargs, json_obj

    @classmethod
    def _parse_response_page(cls, token: str, json_objs: List[dict], page_number: float,
                             all_objs: List[Union['ApexDataObject', int]],
                             archived: bool = False, ids_only: bool = False):
        """
        Parses a single page of a GET response and populates the `all_objs`
        list with either `ApexDataObject` objects or their ImportUserIds
        depending on the value of `ids_only`. Returned only ImportUserIds
        is far more efficient as returning the the objects requires making
        GET calls for each objects whereas the IDs are given in a single
        (paginated) call .

        :param token: Apex access token
        :param json_objs: the objects in the response page
        :param all_objs: the global list of all objects collected thus far
        :param archived: whether to return archived objects
        :param ids_only: whether to return on the IDs
        :return: a list of all objects or their IDs.
        """
        logger = logging.getLogger(__name__)
        for i, obj in enumerate(json_objs):
            progress = f'page {int(page_number)}:{i + 1}/{len(json_objs)}:total {len(all_objs) + 1}'
            try:
                if not archived and obj['RoleStatus'] == 'Archived':
                    continue  # Don't return archived
                iuid = obj['ImportUserId']
                if not iuid:
                    logger.info('Object has no ImportUserId. Skipping...')
                    continue

                if ids_only:
                    logger.info(f'{progress}:Adding ImportUserId {iuid}.')
                    all_objs.append(int(iuid))
                else:
                    logger.info(f'{progress}:Creating {cls.__name__} with ImportUserId {iuid}')
                    apex_obj = cls.get(token, import_id=iuid)
                    all_objs.append(apex_obj)
            except exceptions.ApexObjectNotFoundException:
                error_msg = f'Could not retrieve object of type {cls.__name__} \
                            bearing ImportID {obj["ImportUserID"]}. Skipping object'
                logger.info(error_msg)
            except exceptions.ApexMalformedEmailException as e:
                logger.info(e)
            except KeyError:
                pass

    def to_dict(self) -> dict:
        """Converts attributes to a dictionary."""
        return self.__dict__

    def to_json(self) -> dict:
        """
        Converts instance attributes to dictionary and modifies their contents
        to prepare them for submission to the Apex API.
        """
        json_obj = {}
        for key, value in self.to_dict().items():
            if value is None:
                value = 'null'
            json_obj[utils.snake_to_camel(key)] = value
        json_obj['Role'] = self.role
        return json_obj

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return f'{self.__class__.__name__}({str(self)})'


class ApexStudent(ApexDataObject):

    """
    Represents a student in the Apex database.

    :param Union[str, int] import_user_id: identifier for the database, common
                                           to Apex and PowerSchool
    :param Union[str, int] import_org_id: the school to which the student belongs
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

    def __init__(self, import_user_id: Union[int, str], import_org_id: Union[int, str],
                 first_name: str, middle_name: str, last_name: str, email: str,
                 grade_level: int):
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
        # TODO: Add graduation year?

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

    def get_enrollments(self, token: str) -> Optional[List['ApexClassroom']]:
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

    def get_enrollment_ids(self, token: str) -> List[int]:
        """
        Gets the `ImportClassroomId` of all objects in which the student
        is enrolled. Differs from the `get_enrollments` in that it only returns
        the IDs instead of `ApexClassroom` objects. This makes it a great deal
        faster because only a single call to Apex is made.


        :param token: an Apex access token
        :return: a list of IDs for each classroom in which the student is enrolled
        :rtype: List[int]
        """
        header = get_header(token)
        r = requests.get(url=self.classroom_url, headers=header, params={'isActiveOnly': True})
        try:
            r.raise_for_status()
            return [int(student['ImportClassroomId']) for student in r.json()]
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException(self.import_user_id)
        except KeyError:
            raise exceptions.ApexMalformedJsonException(r.json())

    def transfer(self, token: str, old_classroom_id: str,
                 new_classroom_id: str, new_org_id: str = None) -> Response:
        """
        Transfers student along with role and grade data from one classroom to another

        :param token: Apex access token
        :param old_classroom_id: id of current classroom
        :param new_classroom_id: id of the classroom to which the student will be transferred
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

    def enroll(self, token: str, classroom_id: str) -> Response:
        """
        Enrolls this :class:`ApexStudent` object into the class indexed by `classroom_id`.

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


class ApexStaffMember(ApexDataObject):

    """
    Represents a staff member (likely a teacher) in the Apex database.

    :param Union[int, str] import_user_id:
                            identifier for the database, common to
                            Apex and PowerSchool
    :param Union[int, str] import_org_id: the school to which teacher
                                          staff member belongs
    :param str first_name: the staff member's first/given name
    :param str middle_name: the staff member's middle name
    :param str last_name: the staff member's last/surname
    :param str email: the staff member's school email address (optional)
    :param str login_id: the staff member's login ID
    """

    url = urljoin(BASE_URL, 'staff')
    role = 'T'
    role_set = {'M', 'T', 'TC', 'SC'}
    post_heading = 'staffUsers'
    """
    m = mentor
    t = teacher
    tc = technical coordinator
    sc = site_coordinator
    """

    ps2apex_field_map = {
        'teacher_id': 'import_user_id',
        'school_id': 'import_org_id',
        'teacher_number': 'login_id',
        'email': 'email',
        'first_name': 'first_name',
        'middle_name': 'middle_name',
        'last_name': 'last_name'
    }

    def __init__(self, import_user_id: Union[int, str], import_org_id: Union[int, str],
                 first_name: str, middle_name: str, last_name: str, email: str,
                 login_id: str, login_password: str):
        super().__init__(import_user_id, import_org_id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.login_id = login_id
        self.login_pw = login_password  # TODO: Don't pass password

    @classmethod
    def from_powerschool(cls, json_obj) -> 'ApexStaffMember':
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        kwargs['login_password'] = 'default_password'

        return cls(**kwargs)

    def get_with_orgs(self, token) -> List['ApexStaffMember']:
        """
        Exactly the same as the `get` method with the difference that if a staff member belongs to multiple
        organizations, this method will return a new `ApexStaffMember` object for each organization.

        :param token: Apex access token
        :return:
        """
        # TODO
        pass

    @classmethod
    def _parse_get_response(cls, r) -> 'ApexStaffMember':
        # TODO
        print(r.text)

    def get_classrooms(self, token) -> List['ApexClassroom']:
        # TODO
        pass

    def to_json(self) -> dict:
        json_obj = super().to_json()
        if json_obj['Email'] == 'null':
            json_obj['Email'] = 'dummy@malad.us'
        return json_obj


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


def make_userid(first_name: str, last_name: str):
    """Makes a UserId from first and last names."""
    userid = re.sub(PUNC_REGEX, '', last_name.lower())[:4]
    userid += re.sub(PUNC_REGEX, '', first_name.lower())[:4]
    return userid


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
        distance = utils.levenshtein_distance(t1, t2_name.lower())
        if distance < min_distance:
            min_distance = distance
            argmax = i

    return teachers[argmax]


def get_products(token, program_code: str) -> Response:
    """
    Gets all the products (i.e., curricula) for a given program

    :param token: Apex access token
    :param program_code: the code for the given program
    :return:
    """
    url = urljoin(BASE_URL, 'products/')
    url = urljoin(url, program_code)
    header = get_header(token)
    return requests.get(url=url, headers=header)
