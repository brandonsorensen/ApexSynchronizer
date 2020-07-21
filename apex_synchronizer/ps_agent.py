"""
The module servers as an interface for querying the PowerSchool
database. It defines a function obtaining an PowerSchool token
from the environment and a function for using that token to fetch
a query from a given URL. Additionally, there are four wrapper functions
that simply call the `_fetch_powerquery` function for an available
URL. In practice, these are the functions that compose the API.

The `course2program_code`, aptly named, maps a course code as defined
in the documentation provided by Apex Learning to their respective
program codes.
"""

import logging
import os
from urllib.parse import urljoin

import requests

from .exceptions import PSEmptyQueryException, PSNoConnectionError
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
    """
    Obtains an access token and calls a PowerQuery at a given url,
    limiting it to `page_size` results.

    :param url_ext: the extension that, appended to the `PS_URL`
        environment variable and `BASE_URL` as defined above,
        composes the URL
    :param page_size: how many results to return, 0 = all
    :raises PSEmptyQueryException: when no results are returned
    :return: the JSON object returned by the PowerQuery
    """
    logger = logging.getLogger(__name__)
    logger.info('Fetching PowerQuery with extension ' + str(url_ext))
    token = get_ps_token()
    header = get_header(token, custom_args={'Content-Type': 'application/json'})
    payload = {'pagesize': page_size}
    url = urljoin(os.environ['PS_URL'], BASE_QUERY_URL + url_ext)

    r = requests.post(url, headers=header, params=payload)
    logger.info('PowerQuery returns with status ' + str(r.status_code))
    try:
        return r.json()['record']
    except KeyError as e:
        if e.args[0] == 'record':
            raise PSEmptyQueryException(url)
        raise e


def get_ps_token() -> str:
    """
    Gets the PowerSchool access token from the PowerSchool server using
    the following environment variables:

        - PS_CLIENT_ID: the given client ID for the PowerSchool plugin
        - PS_CLIENT_SECRET: the secret code
        - PS_URL: the PowerSchool URL

    :return: an access token for the PowerSchool server
    """
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

    r = requests.post(url, headers=header, data=payload)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise PSNoConnectionError()
    
    return r.json()['access_token']


