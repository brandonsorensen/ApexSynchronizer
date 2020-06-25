import os
import re
import requests
from urllib.parse import urljoin


BASE_URL = 'https://api.apexvs.com/'


def get_header(token, custom_args: dict = None) -> dict:
    header = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    if custom_args is not None:
        header.update(custom_args)
    return header


def get_ps_token():
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


def snake_to_camel(var):
    """Converts snake case to camel case."""
    return ''.join(ch.capitalize() or '_' for ch in var.split('_'))
