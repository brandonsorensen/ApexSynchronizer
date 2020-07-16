from datetime import datetime, timedelta
import logging
from typing import Union
import os

from requests.auth import HTTPBasicAuth
import requests

from .exceptions import ApexConnectionException
import apex_synchronizer

TokenType = Union[str, 'ApexAccessToken']


class ApexSession(object):

    def __init__(self):
        self._access_token = ApexAccessToken.get_new_token()
        self.logger = logging.getLogger(__name__)
        self.logger.info('Session opened.')

    @property
    def access_token(self) -> 'ApexAccessToken':
        if self._access_token.expiration < datetime.now():
            self.logger.debug('Old token expired. Generating new one.')
            self._access_token = ApexAccessToken.get_new_token()
        return self._access_token


class ApexAccessToken(object):

    PADDING = 10  # token will expire this many seconds before real expiration

    def __init__(self, token_reponse):
        as_json = token_reponse.json()
        self.token = as_json['access_token']

        expires_in = int(as_json['expire_in']) - self.PADDING
        # subtracting `PADDING` seconds to give some leeway
        self.expiration = datetime.now() + timedelta(seconds=expires_in)
        
    @classmethod
    def get_new_token(cls):
        try:
            client_id = os.environ['CONSUMER_KEY']
            secret_key = os.environ['SECRET_KEY']
        except KeyError:
            raise EnvironmentError('ClientID or secret key are not in'
                                   'the environment.')

        logger = logging.getLogger(__name__)
        url = apex_synchronizer.adm.BASE_URL + 'token'
        request_json = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret':secret_key 
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
