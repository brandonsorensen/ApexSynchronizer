"""
A convenience module for quickly looking up objects in PowerSchool and
returning them as ApexDataObjects. This has no pracical use in the
synchronizer, but is useful for debugging and quick interactions with
the PS and Apex interfaces. Documentation will be sparse as code should
be straightforward and self-explanatory.

Created a module separate from the ps_agent module to avoid circular
imports and to separate utility to the synchronizer from helper
functions.
"""

from typing import Union

from . import adm
from .ps_agent import fetch_classrooms, fetch_students
from .utils import flatten_ps_json


def get_student_for_eduid(eduid: int) -> adm.ApexStudent:
    return _get_apex_student(str(eduid), apex_key='eduid')


def get_student_for_email(email: str) -> adm.ApexStudent:
    return _get_apex_student(email, apex_key='email')


def get_classroom_for_id(c_id: int) -> adm.ApexClassroom:
    for classroom_obj in map(flatten_ps_json, fetch_classrooms()):
        if int(classroom_obj['section_id']) == c_id:
            return adm.ApexClassroom.from_powerschool(classroom_obj, already_flat=True)


def _get_apex_student(id_: Union[int, str], apex_key: str) -> adm.ApexStudent:
    for student_obj in map(flatten_ps_json, fetch_students()):
        if student_obj[apex_key] == id_:
            return adm.ApexStudent.from_powerschool(student_obj, already_flat=True)
