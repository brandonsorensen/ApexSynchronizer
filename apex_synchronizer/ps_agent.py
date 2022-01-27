"""
This module servers as an interface for querying the PowerSchool
database. It defines a function obtaining an PowerSchool token
from the environment and a class that uses said token to fetch
a query from a given URL. Additionally, there are four
:class:`PowerQuery` objs that simply call the four PowerQueries
available to the Apex plugin. In practice, these are the functions that
compose the API.

The :data:`course2program_code`, aptly named, maps a course code as
defined in the documentation provided by Apex Learning to their
respective program codes.
"""

import logging
import os
from urllib.parse import urljoin

import requests

from .exceptions import PSEmptyQueryException, PSNoConnectionError
from .utils import get_header, flatten_ps_json


course2program_code = {
    616: 'Z1707458',
    615: 'Z7250853',
    501: 'Z9065429',
    601: 'Z1001973'
}


class PowerQuery(object):
    """
    Represents a PowerQuery call to the PowerSchool server. A
    PowerQuery is a custom SQL statement that is defined in by a
    PowerSchool plugin XML file. Four are defined for the Apex Learning
    Plugin, and can be accessed by appending the following four key
    words to PowerSchool URL + :data:`BASE_QUERY_URL` combination:

        - classrooms
        - enrollment
        - students
        - teachers

    These stock PowerQuery objects are defined in this :mod:`ps_agent`
    module.

    :cvar BASE_QUERY_URL: the base URL schema for location the Apex
        PowerQueries.
    """

    BASE_QUERY_URL = '/ws/schema/query/com.apex.learning.school.'

    def __init__(self, url_ext: str, description: str = None):
        """
        :param str url_ext: the extension that, appended to the `PS_URL`
            environment variable and `BASE_URL` as defined above,
            composes the URL
        """
        self.url_ext = url_ext
        if description is not None:
            self.__doc__ = description

    def fetch(self, page_size: int = 0) -> dict:
        """
        Obtains an access token and calls a PowerQuery at a given url,
        limiting it to `page_size` results.

        :param int page_size: how many results to return, 0 = all
        :raises PSEmptyQueryException: when no results are returned
        :return: the JSON object returned by the PowerQuery
        """
        logger = logging.getLogger(__name__)
        logger.info('Fetching PowerQuery with extension ' + str(self.url_ext))
        token = get_ps_token()
        header = get_header(token,
                            custom_args={'Content-Type': 'application/json'})
        payload = {'pagesize': page_size}
        url = urljoin(os.environ['PS_URL'], self.BASE_QUERY_URL + self.url_ext)

        r = requests.post(url, headers=header, params=payload)
        r.raise_for_status()
        logger.debug('PowerQuery returns with status ' + str(r.status_code))
        try:
            return r.json()['record']
        except KeyError as e:
            if e.args[0] == 'record':
                raise PSEmptyQueryException(url)
            raise e

    def __call__(self, page_size=0) -> dict:
        """Calls the fetch method."""
        return self.fetch(page_size=page_size)


fetch_classrooms = PowerQuery('classrooms')
fetch_enrollment = PowerQuery('enrollment')
fetch_staff = PowerQuery('teachers')
fetch_students = PowerQuery('students')


def get_eduid_map():
    students = fetch_students()
    student2eduid = {}
    for student in map(flatten_ps_json, students):
        try:
            student2eduid[student['email']] = int(student['eduid'])
        except (TypeError, ValueError):
            pass

    return student2eduid


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
    except requests.exceptions.HTTPError:
        raise PSNoConnectionError()
    
    return r.json()['access_token']
