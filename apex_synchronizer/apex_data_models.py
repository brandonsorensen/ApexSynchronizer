import requests
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List
from .utils import BASE_URL, get_header


class ApexDataObject(object):

    @staticmethod
    @abstractmethod
    def get(token, import_id) -> 'ApexDataObject':
        pass

    @staticmethod
    @abstractmethod
    def get_all(token) -> List['ApexDataObject']:
        pass

    def to_dict() -> dict:
        return self.__dict__


class Student(object):

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
    
    def get(token, user_id: int) -> 'Student':
        pass

    def get_all(token) -> List['Student']:
        url = BASE_URL + 'students'
        r = requests.get(url=url, headers=get_header(token))
        print(r.text)


class ApexDataObjectException(Exception):
    
    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object of type {type(self.object)} could not be retrieved.'

