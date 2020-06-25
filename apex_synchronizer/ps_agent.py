import os
import requests
from urllib.parse import urljoin
from .utils import get_ps_token, get_header


def fetch_classrooms():
    token = get_ps_token()
    header = get_header(token, custom_args={'Content-Type': 'application/json'})
    payload = {'pagesize': 0}
    url = urljoin(os.environ['PS_URL'],
            '/ws/schema/query/com.apex.learning.school.classrooms')

    r = requests.post(url, headers=header, params=payload)
    return r.json()

