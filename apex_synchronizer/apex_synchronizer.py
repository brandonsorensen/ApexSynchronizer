import os
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from utils import BASE_URL


class ApexSynchronizer(object):

    def __init__(self):
        self._token = ApexAccessToken.get_new_token()


    @property
    def token(self):
        if self._token.expiration < datetime.now():
            self._token = ApexAccessToken.get_new_token()
        return self._token


class ApexAccessToken(object):

    def __init__(self, token_reponse):
        as_json = token_reponse.json()
        self.token = as_json['access_token']

        expires_in = int(as_json['expire_in'])
        # subtracting ten seconds to give some leeway
        self.expiration = datetime.now() + timedelta(seconds=expires_in - 10)
        
    @staticmethod
    def get_new_token():
        try:
            client_id = os.environ['CONSUMER_KEY']
            secret_key = os.environ['SECRET_KEY']
        except KeyError:
            raise EnvironmentError('ClientID or secret key are not in the environment.')

        url = BASE_URL + 'token'
        request_json = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret':secret_key 
        }

        headers = {"Accept": "application/json"}
        auth = HTTPBasicAuth(client_id, secret_key)
        r = requests.post(url, json=request_json, headers=headers, auth=auth)
        if r.status_code == 200:
            return ApexAccessToken(r)

        # TODO: Add proper exception handling
        raise Exception('Could not get token')
