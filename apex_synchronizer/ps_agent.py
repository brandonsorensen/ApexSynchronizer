import os
import requests
from collections import defaultdict
from typing import Iterable, Set, Union
from urllib.parse import urljoin
from .utils import get_header, flatten_ps_json


BASE_QUERY_URL = '/ws/schema/query/com.apex.learning.school.'


course2program_code = {
    616: 'Z8102253',
    615: 'Z9830940',
    501: 'Z3844077',
    601: 'Z2227630'
}


class PSEnrollment(object):

    def __init__(self, ps_json=None):
        if ps_json is None:
            ps_json = fetch_enrollment()

        json_obj = map(flatten_ps_json, ps_json)
        self._parse_json(json_obj)

    def get_classrooms(self, eduid: Union[int, str]) -> Set[int]:
        """
        Returns all classrooms in which a given student is enrolled. Students
        are indexed by their EDUIDs, which may be given as an int or a
        numeric string.

        :param Union[int, str] eduid: the EDUID of a given student
        :return: all classrooms in which the student is enrolled
        :rtype: set[int]
        """
        return self.student2classrooms[int(eduid)]

    def get_roster(self, section_id: Union[int, str]) -> Set[int]:
        """
        Returns the roster of a given classroom, indexed by its section ID.
        Section IDs may be given as integers or numeric strings.

        :param Union[int, str] section_id: the section ID of the classroom
        :return: the EDUIDs of all students in the classroom
        :rtype: set[int]
        """
        return self.classroom2students[int(section_id)]

    def _parse_json(self, json_obj: Iterable[dict]):
        """Parses a JSON object returned by the `fetch_enrollment` function."""
        self.student2classrooms = defaultdict(set)
        self.classroom2students = defaultdict(set)

        for entry in json_obj:
            eduid = int(entry['eduid'])
            sec_id = int(entry['section_id'])

            self.student2classrooms[eduid].add(sec_id)
            self.classroom2students[sec_id].add(eduid)

        self.student2classrooms = dict(self.student2classrooms)
        self.classroom2students = dict(self.classroom2students)


def fetch_classrooms() -> dict:
    return _fetch_powerquery('classrooms')


def fetch_staff() -> dict:
    return _fetch_powerquery('teachers')


def fetch_students() -> dict:
    return _fetch_powerquery('students')


def fetch_enrollment() -> dict:
    return _fetch_powerquery('enrollment')


def _fetch_powerquery(url_ext: str, page_size=0) -> dict:
    token = get_ps_token()
    header = get_header(token, custom_args={'Content-Type': 'application/json'})
    payload = {'pagesize': page_size}
    url = urljoin(os.environ['PS_URL'], BASE_QUERY_URL + url_ext)

    r = requests.post(url, headers=header, params=payload)
    return r.json()['record']


def get_ps_token() -> str:
    header = {
        'Content-Type': "application/x-www-form-urlencoded;charset=UTF-8'"
    }
    try:
        client_id = os.environ['PS_CLIENT_ID']
        client_secret = os.environ['PS_CLIENT_SECRET']
        url = urljoin(os.environ['PS_URL'], '/oauth/access_token')
    except ValueError:
        raise EnvironmentError('PowerSchool credentials are not in the environment.')

    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }

    try:
        r = requests.post(url, headers=header, data=payload)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
    return r.json()['access_token']


