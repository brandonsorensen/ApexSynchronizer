import logging
import os
from urllib.parse import urljoin

import requests

from .utils import get_header

BASE_QUERY_URL = '/ws/schema/query/com.apex.learning.school.'

course2program_code = {
    616: 'Z1707458',
    615: 'Z7250853',
    501: 'Z9065429',
    601: 'Z1001973'
}


def fetch_classrooms() -> dict:
    return _fetch_powerquery('classrooms')


def fetch_staff() -> dict:
    return _fetch_powerquery('teachers')


def fetch_students() -> dict:
    return _fetch_powerquery('students')


def fetch_enrollment() -> dict:
    return _fetch_powerquery('enrollment')


def _fetch_powerquery(url_ext: str, page_size=0) -> dict:
    logger = logging.getLogger(__name__)
    logger.info('Fetching PowerQuery with extension ' + str(url_ext))
    token = get_ps_token()
    header = get_header(token, custom_args={'Content-Type': 'application/json'})
    payload = {'pagesize': page_size}
    url = urljoin(os.environ['PS_URL'], BASE_QUERY_URL + url_ext)

    r = requests.post(url, headers=header, params=payload)
    logger.info('PowerQuery returns with status ' + str(r.status_code))
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
        raise EnvironmentError('PowerSchool credentials are not in the'
                               'environment.')

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


