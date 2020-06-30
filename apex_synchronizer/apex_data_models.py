import requests
import json
from abc import ABC, abstractmethod
from datetime import datetime
from .ps_agent import course2program_code, fetch_staff
from typing import List, Union, Tuple
from requests.models import Response
from urllib.parse import urljoin, urlparse
from .utils import BASE_URL, get_header, snake_to_camel, camel_to_snake, levenshtein_distance


APEX_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
PS_DATETIME_FORMAT = '%Y/%m/%d'


class ApexDataObject(ABC):

    def __init__(self, import_user_id, import_org_id):
        self.import_user_id = import_user_id
        self.import_org_id = import_org_id

    @classmethod
    def get(cls, token, import_id) -> 'ApexDataObject':
        r = cls._get_response(token, import_id)
        return cls._parse_get_response(r)

    @classmethod
    @abstractmethod
    def _parse_get_response(cls, r):
        pass

    @classmethod
    def _get_response(cls, token, import_id) -> Response:
        custom_args = {
            'importUserId': import_id
        }
        header = get_header(token, custom_args)
        url = urljoin(cls.url + '/', import_id)
        r = requests.get(url=url, headers=header)
        return r

    @classmethod
    def get_all(cls, token) -> List['ApexDataObject']:
        r = requests.get(url=cls.url, headers=get_header(token))
        json_objs = json.loads(r.text)

        return [cls.get(token, obj['ImportUserId']) for obj in json_objs]

    def post_to_apex(self, token) -> Response:
        return self.post_batch(token, [self])

    @classmethod
    def post_batch(cls, token, classrooms):
        header = get_header(token)
        payload = json.dumps({cls.post_heading: [c.to_json() for c in classrooms]})
        url = cls.url if len(classrooms) <= 50 else urljoin(cls.url, 'batch')
        r = requests.post(url=url, data=payload, headers=header)
        # TODO: Error handling
        return r

    def delete_from_apex(self, token) -> Response:
        custom_args = {
            'importUserId': self.import_user_id,
            'orgId': self.import_org_id
        }
        header = get_header(token, custom_args)
        url = urljoin(self.url + '/', self.import_user_id)
        r = requests.delete(url=url, headers=header)
        return r

    @abstractmethod
    def put_to_apex(self, token) -> Response:
        pass

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
        json_obj = cls._flatten_ps_json(json_obj)
        for ps_key, apex_key in cls.ps2apex_field_map.items():
            kwargs[apex_key] = json_obj[ps_key]
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
            snake_key = camel_to_snake(key)
            if snake_key in params:
                kwargs[snake_key] = value

        return kwargs, json_obj

    @staticmethod
    def _flatten_ps_json(json_obj) -> dict:
        """Takes the 3D dict returned by PowerSchool and flattens it into 1D."""
        flattened = {}
        for table in json_obj['tables'].values():
            flattened.update(table)
        return flattened

    def to_dict(self) -> dict:
        return self.__dict__

    def to_json(self) -> dict:
        json_obj = {}
        for key, value in self.to_dict().items():
            if value is None:
                value = 'null'
            json_obj[snake_to_camel(key)] = value
        json_obj['Role'] = self.role
        return json_obj

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return f'{self.__class__.__name__}({str(self)})'


class ApexStudent(ApexDataObject):
    role = 'S'
    url = urljoin(BASE_URL, 'students')
    post_heading = 'studentUsers'

    ps2apex_field_map = {
        'eduid': 'import_user_id',
        'school_id': 'import_org_id',
        'first_name': 'first_name',
        'middle_name': 'middle_name',
        'last_name': 'last_name',
        'web_id': 'login_id',
        'grade_level': 'grade_level',
        'email': 'email'
    }

    def __init__(self, import_user_id: int, import_org_id: int, first_name: str,
                 middle_name: str, last_name: str, email: str, grade_level: int,
                 login_id: str):
        super().__init__(import_user_id, import_org_id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.grade_level = grade_level
        self.login_id = login_id
        # TODO: Add graduation year?

    @classmethod
    def _parse_get_response(cls, r: Response):
        kwargs, json_obj = cls._init_kwargs_from_get(r)
        # Just returns the first organization in the list
        # Students should only be assigned to a single org
        kwargs['import_org_id'] = json_obj['Organizations'][0]['ImportOrgId']

        return cls(**kwargs)

    def put_to_apex(self, token) -> Response:
        header = get_header(token)
        url = urljoin(self.url + '/', self.import_user_id)
        payload = self.to_json()
        del payload['ImportUserId']  # Given in the URL
        r = requests.put(url=url, headers=header, data=payload)
        return r

    @classmethod
    def from_powerschool(cls, json_obj: dict) -> 'ApexStudent':
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        if kwargs['import_user_id'] is None:
            kwargs['import_user_id'] = '10'
        kwargs['email'] = 'dummy@malad.us'

        return cls(**kwargs)

    def get_enrollments(self, token: str) -> List['ApexClassroom']:
        """
        Gets all classes in which this :class:`ApexStudent` is enrolled.

        :param token: an Apex access token
        :return: a list of ApexClassroom objects
        """
        # TODO
        header = get_header(token)
        r = requests.get(url=self.classroom_url, headers=header)
        print(r.text)

    def transfer(self, token: str, old_classroom_id: str,
                 new_classroom_id: str, new_org_id: str = None) -> Response:
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

    @property
    def classroom_url(self) -> str:
        url = urljoin(self.url + '/', self.import_user_id)
        url = urljoin(url + '/', 'classrooms')
        return url


class ApexStaffMember(ApexDataObject):
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

    def __init__(self, import_user_id: str, import_org_id: str, first_name: str,
                 middle_name: str, last_name: str, email: str, login_id: str,
                 login_password: str):
        super().__init__(import_user_id, import_org_id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.login_id = login_id
        self.login_pw = login_password

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

    def put_to_apex(self, token) -> Response:
        pass

    @classmethod
    def _parse_get_response(cls, r) -> 'ApexStaffMember':
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

    def put_to_apex(self, token: str) -> Response:
        pass

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

    def enroll(self, token: str,
               objs: Union[List[ApexDataObject], ApexDataObject]) -> Response:
        if issubclass(type(objs), ApexDataObject):
            dtype = type(objs)
            objs = [objs]
        else:
            # Assuming that a non-empty list of objects is passed.
            assert len(objs) > 0
            dtype = type(objs[0])

        header = get_header(token)
        url = urljoin(self.url + '/', self.import_classroom_id)
        # Get the final component in the object's url
        obj_type_component = urlparse(dtype.url).path.rsplit("/", 1)[-1]
        url = urljoin(url + '/', obj_type_component)

        payload = {dtype.post_heading: []}
        for apex_obj in objs:
            payload_entry = {
                'ImportUserId': apex_obj.import_user_id,
                'ImportOrgId': apex_obj.import_org_id
            }
            payload[dtype.post_heading].append(payload_entry)

        payload = json.dumps(payload)
        return requests.post(url=url, headers=header, data=payload)


def teacher_fuzzy_match(t1: str) -> ApexStaffMember:
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


class ApexDataObjectException(Exception):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object of type {type(self.object)} could not be retrieved.'


class NoUserIdException(ApexDataObjectException):

    def __str__(self):
        return 'Object does not have an ImportUserID.'


class DuplicateUserException(ApexDataObjectException):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object with user id {self.object.import_user_id} already exists.'


def get_products(token, program_code: str) -> Response:
    url = urljoin(BASE_URL, 'products/')
    url = urljoin(url, program_code)
    header = get_header(token)
    return requests.get(url=url, headers=header)
