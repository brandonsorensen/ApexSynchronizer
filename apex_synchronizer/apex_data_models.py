import requests
import json
from abc import ABC, abstractmethod
from collections import namedtuple
from datetime import datetime
from .ps_agent import course2program_code
from typing import List
from requests.models import Response
from urllib.parse import urljoin
from .utils import BASE_URL, get_header, snake_to_camel


class ApexDataObject(ABC):

    def __init__(self, import_user_id):
        self.import_user_id = import_user_id

    @classmethod
    @abstractmethod
    def get(cls, token, import_id) -> 'ApexDataObject':
        pass

    @classmethod
    def get_all(cls, token) -> List['ApexDataObject']:
        r = requests.get(url=cls.url, headers=get_header(token))
        print(r.text)

    def post_to_apex(self, token) -> Response:
        return self.post_batch(token, [self])

    @staticmethod
    @abstractmethod
    def post_batch(token, objs: List['ApexDataObject']) -> Response:
        pass

    @abstractmethod
    def put_to_apex(self, token) -> Response:
        pass

    @property
    @abstractmethod
    def url(self):
        pass

    @classmethod 
    @abstractmethod 
    def from_powerschool(cls, ps_json):
        pass

    def to_dict(self) -> dict:
        return self.__dict__

    def to_json(self) -> dict:
        return {snake_to_camel(key): value for key, value in self.to_dict().items()}


class ApexStudent(ApexDataObject):

    role = 'S'
    url = urljoin(BASE_URL, 'students')

    def __init__(self, import_user_id: int, import_org_id: int, first_name: str,
                 middle_name: str, last_name: str, email: str, grade_level: int,
                 login_id: str, login_password: str, coach_emails: str):

        super().__init__(import_user_id)
        self.import_org_id = import_org_id
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.grade_level = grade_level
        self.login_id = login_id
        self.login_password = login_password
        self.coach_emails = coach_emails
    
    def get(cls, token, user_id: int) -> 'ApexStudent':
        pass


class ApexStaffMember(ApexDataObject):

    url = urljoin(BASE_URL, 'staff')
    role_set = set(['M', 'T', 'TC', 'SC'])
    """
    m = mentor
    t = teacher
    tc = technical coordinator
    sc = site_coordinator
    """

    def __init__(self, import_user_id: str, import_org_id: str, first_name: str,
                 middle_name: str, last_name: str, email: str, login_id: str,
                 login_password: str, role: str):
        super().__init__(import_user_id)
        self.import_org_id = import_org_id
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.login_id = login_id
        self.login_pw = login_password
        self.role = role

        if self.role not in self.role_set:
            raise ValueError(f'Role must be one of {self.role_set}')

    @staticmethod
    def get(token, user_id):
        # TODO
        pass

    def put_to_apex(self, token):
        # TODO
        pass

    @staticmethod
    def post_batch(token, staff_members):
        header = get_header(token)
        payload = json.dumps({'staffUsers': [mem.to_json() for mem in staff_members]})
        r = requests.post(url=ApexStaffMember.url, data=payload, headers=header)
        # TODO: Error handling
        return r

    def get_classrooms(token):
        # TODO
        pass


class ApexClassroom(ApexDataObject):

    url = urljoin(BASE_URL, 'classrooms')
    role = 'T'
    ps2apex_field_map = {
        'first_day': 'classroom_start_date',
        'teacher_id': 'import_user_id',
        'school_id': 'import_org_id',
        'section_id': 'import_classroom_id',
    }

    def __init__(self, import_org_id: int, import_classroom_id: int,
                 classroom_name: str, product_codes: [str], import_user_id: int,
                 classroom_start_date: str, program_code: str):
        super().__init__(import_user_id)
        self.import_org_id = import_org_id
        self.import_classroom_id = import_classroom_id
        self.classroom_name = classroom_name
        self.product_codes = product_codes
        self.classroom_start_date = classroom_start_date
        self.program_code = program_code
        # Can either be `datetime` or `str`
#       if not isinstance(self.classroom_start_date, datetime):
#           self.classroom_start_date = datetime.strptime(self.classroom_start_date,
#                                                         '%Y-%m-%d')

    @staticmethod
    def get(token, user_id):
        # TODO
        pass

    @staticmethod
    def post_batch(token, classrooms):
        header = get_header(token)
        payload = json.dumps({'classroomEntries': [c.to_json() for c in classrooms]})
        url = ApexClassroom.url if len(classrooms) <= 50 else urljoin(self.url, 'batch')
        r = requests.post(url=url, data=payload, headers=header)
        # TODO: Error handling
        return r

    def get_classrooms(token):
        # TODO
        pass

    def put_to_apex(self, token) -> Response:
        pass

    @classmethod
    def from_powerschool(cls, json_obj):
        kwargs = {}
        json_obj = cls._flatten_ps_json(json_obj)
        for ps_key, apex_key in cls.ps2apex_field_map.items():
            kwargs[apex_key] = json_obj[ps_key]

        kwargs['classroom_name'] = json_obj['course_name'] + ' - ' \
                + json_obj['section_number']

        kwargs['program_code'] = course2program_code[int(json_obj['school_id'])]
        kwargs['product_codes'] = [json_obj['apex_program_code']]

        return cls(**kwargs)

    @staticmethod
    def _flatten_ps_json(json_obj):
        flattened = {}
        for table in json_obj['tables'].values():
            flattened.update(table)
        return flattened

class ApexDataObjectException(Exception):
    
    def __init__(self, obj):
        self.object = obj


    def __str__(self):
        return f'Object of type {type(self.object)} could not be retrieved.'


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

