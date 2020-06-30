import requests
import json
from abc import ABC, abstractmethod
from collections import namedtuple
from .ps_agent import course2program_code
from typing import List
from requests.models import Response
from urllib.parse import urljoin
from .utils import BASE_URL, get_header, snake_to_camel, camel_to_snake


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
        pass

    @property
    @abstractmethod
    def post_heading(self) -> str:
        pass

    @property
    @abstractmethod
    def role(self) -> str:
        pass

    @property
    @abstractmethod
    def ps2apex_field_map(self) -> dict:
        pass

    @classmethod
    def _init_kwargs_from_ps(cls, json_obj):
        kwargs = {}
        json_obj = cls._flatten_ps_json(json_obj)
        for ps_key, apex_key in cls.ps2apex_field_map.items():
            kwargs[apex_key] = json_obj[ps_key]
        return kwargs

    @staticmethod
    def _flatten_ps_json(json_obj) -> dict:
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
    def _parse_get_response(cls, r):
        json_obj = json.loads(r.text)
        kwargs = {}
        params = set(cls.ps2apex_field_map.values())
        for key, value in json_obj.items():
            snake_key = camel_to_snake(key)
            if snake_key in params:
                kwargs[snake_key] = value

        # Just returns the first organization in the list
        # Students should only be assigned to a single org
        kwargs['import_org_id'] = json_obj['Organizations'][0]['ImportOrgId']

        return cls(**kwargs)

    def put_to_apex(self, token) -> Response:
        header = get_header(token)
        url = urljoin(self.url + '/', self.import_user_id)
        payload = self.to_json()
        del payload['ImportUserId']  # Given in the URL
        print(payload)
        r = requests.put(url=url, headers=header, data=payload)
        return r

    @classmethod
    def from_powerschool(cls, json_obj):
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        if kwargs['import_user_id'] is None:
            kwargs['import_user_id'] = '10'
        kwargs['email'] = 'dummy@malad.us'

        return cls(**kwargs)

    def get_enrollments(self):
        pass

    def transfer(self, new_classroom_id):
        pass


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
    def from_powerschool(cls, json_obj):
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        kwargs['login_password'] = 'default_password'

        return cls(**kwargs)

    def get_with_orgs(self, token) -> List['ApexClassroom']:
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
    def _parse_get_response(cls, r) -> 'ApexClassroom':
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

    def __init__(self, import_org_id: int, import_classroom_id: int,
                 classroom_name: str, product_codes: [str], import_user_id: int,
                 classroom_start_date: str, program_code: str):
        super().__init__(import_user_id, import_org_id)
        self.import_classroom_id = import_classroom_id
        self.classroom_name = classroom_name
        self.product_codes = product_codes
        self.classroom_start_date = classroom_start_date
        self.program_code = program_code

    @classmethod
    def from_powerschool(cls, json_obj):
        kwargs = cls._init_kwargs_from_ps(json_obj)
        kwargs['classroom_name'] = kwargs['course_name'] + ' - ' + kwargs['section_number']
        del kwargs['course_name']
        del kwargs['section_number']

        kwargs['program_code'] = course2program_code[int(kwargs['import_org_id'])]
        kwargs['product_codes'] = [kwargs['product_codes']]

        return cls(**kwargs)

    def put_to_apex(self, token) -> Response:
        pass

    @classmethod
    def _parse_get_response(cls, r) -> 'ApexClassroom':
        pass


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
