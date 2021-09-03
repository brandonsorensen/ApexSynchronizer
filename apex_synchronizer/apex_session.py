from datetime import datetime, timedelta
import logging
from typing import Union
import os

from requests.auth import HTTPBasicAuth
import requests

from .exceptions import ApexConnectionException
import apex_synchronizer

TokenType = Union[str, 'ApexAccessToken']


class ApexSession(requests.Session):

    """
    Extends the regular :class:`requests.Session` class to automatically
    generate an access token for the Apex API based on credentials in
    the environment.

    :ivar logging.Logger logger: module-wide logger, accessed by
        __name__
    :ivar ApexAccessToken access_token: token for accessing the Apex
        API
    """

    def __init__(self):
        """
        Initializes instance variables from `super` and creates
        an access token.
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Session opened.')
        self._access_token = None
        self.update_token()

    def __del__(self):
        self.close()

    @property
    def access_token(self) -> 'ApexAccessToken':
        """Automatically renews the access token when it expires."""
        if self._access_token.expired():
            self.update_token()
        return self._access_token

    def update_token(self):
        if self._access_token is not None:
            self.logger.debug('Old token expired. Generating new one.')
        self._access_token = ApexAccessToken.get_new_token()
        self.headers.update(
            apex_synchronizer.utils.get_header(self._access_token)
        )


class ApexAccessToken(object):

    """
    Represents an access token for the Apex API, which is generated
    with the following environment variables:

    - CONSUMER_KEY
    - SECRET_KEY

    :param datetime expires_in: the time at which the token expires
    """
    # token will expire this many seconds before real expiration
    _PADDING = 10

    def __init__(self, token_reponse):
        as_json = token_reponse.json()
        self.token = as_json['access_token']

        expires_in = int(as_json['expire_in']) - self._PADDING
        # subtracting `PADDING` seconds to give some leeway
        self.expiration = datetime.now() + timedelta(seconds=expires_in)

    def expired(self):
        return self.expiration < datetime.now()
        
    @classmethod
    def get_new_token(cls):
        """Creates a new access token."""
        try:
            client_id = os.environ['CONSUMER_KEY']
            secret_key = os.environ['SECRET_KEY']
        except KeyError:
            raise EnvironmentError('ClientID or secret key are not in '
                                   'the environment.')

        logger = logging.getLogger(__name__)
        url = apex_synchronizer.adm.BASE_URL + 'token'
        request_json = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': secret_key
        }

        headers = {"Accept": "application/json"}
        auth = HTTPBasicAuth(client_id, secret_key)
        r = requests.post(url, json=request_json, headers=headers, auth=auth)
        try:
            r.raise_for_status()
            logger.debug('Successfully retrieved new token.')
            return cls(r)
        except requests.exceptions.HTTPError:
            logger.exception('Apex server could not be reached.')
            raise ApexConnectionException()

    def __str__(self):
        return self.token

    def __repr__(self):
        return f'ApexAccessToken({self.token})'
