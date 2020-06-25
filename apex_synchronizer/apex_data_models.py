import requests
import json
from abc import ABC, abstractmethod
from collections import namedtuple
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

