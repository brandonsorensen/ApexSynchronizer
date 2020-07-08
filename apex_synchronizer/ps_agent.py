import os
import requests
from collections import defaultdict
from urllib.parse import urljoin
from .utils import get_header, flatten_ps_json


BASE_QUERY_URL = '/ws/schema/query/com.apex.learning.school.'


class PSEnrollment(object):

    def __init__(self, ps_json=None):
        if ps_json is None:
            ps_json = fetch_enrollment()

        json_obj = flatten_ps_json(ps_json)
        self._parse_json(json_obj)

    def get_classrooms(self, eduid):
        return self.student2classrooms[eduid]

    def get_roster(self, section_id):
        return self.classroom2students[section_id]

    def _parse_json(self, json_obj):
        self.student2classrooms = defaultdict(set)
        self.classroom2students = defaultdict(set)

        for sec_id, eduid in json_obj:
            self.student2classrooms[eduid].add(sec_id)
            self.classroom2students[sec_id].add(eduid)


def fetch_classrooms() -> dict:
    return _fetch_powerquery(BASE_QUERY_URL + 'classrooms')


def fetch_staff() -> dict:
    return _fetch_powerquery(BASE_QUERY_URL + 'teachers')


def fetch_students() -> dict:
    return _fetch_powerquery(BASE_QUERY_URL + 'students')


def fetch_enrollment() -> dict:
    return _fetch_powerquery(BASE_QUERY_URL + 'enrollment')


def _fetch_powerquery(url, page_size=0) -> dict:
    token = get_ps_token()
    header = get_header(token, custom_args={'Content-Type': 'application/json'})
    payload = {'pagesize': page_size}
    url = urljoin(os.environ['PS_URL'], url)

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


course2program_code = {
    616: 'Z8102253',
    615: 'Z9830940',
    501: 'Z3844077',
    601: 'Z2227630'
}

