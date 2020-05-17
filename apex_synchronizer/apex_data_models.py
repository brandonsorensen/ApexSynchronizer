import requests
import json
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List
from requests.models import Response
from .utils import BASE_URL, get_header


class ApexDataObject(object):

    @classmethod
    @abstractmethod
    def get(cls, token, import_id) -> 'ApexDataObject':
        pass

    @classmethod
    @abstractmethod
    def get_all(cls, token) -> List['ApexDataObject']:
        pass

    @abstractmethod
    def post_to_apex(self) -> Response:
        pass

    @staticmethod
    @abstractmethod
    def post_batch(self) -> Response:
        pass

    @abstractmethod
    def put_to_apex(self) -> Reponse:
        pass

    def to_dict() -> dict:
        return self.__dict__


class ApexStudent(object):

    role = 'S'

    def __init__(self, import_user_id: int, import_org_id: int, first_name: str,
                 middle_name: str, last_name: str, email: str, grade_level: int,
                 login_id: str, login_password: str, coach_emails: str):

        self.import_user_id = import_user_id
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

    def get_all(cls, token) -> List['ApexStudent']:
        url = BASE_URL + 'students'
        r = requests.get(url=url, headers=get_header(token))
        print(r.text)


class ApexStaffMember(ApexDataObject):

    def __init__(self):
        pass


class ApexDataObjectException(Exception):
    
    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object of type {type(self.object)} could not be retrieved.'

