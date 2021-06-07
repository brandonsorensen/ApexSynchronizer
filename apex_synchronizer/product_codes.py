from urllib.parse import urljoin

from .apex_session import ApexSession
from .apex_data_models import BASE_URL
from .utils import get_header


def get_product_codes(session: ApexSession, program_code: str) -> dict:
    url = urljoin(BASE_URL, 'products/')
    url = urljoin(url, program_code)
    r = session.get(url, headers=get_header(session.access_token))
    return r.json()
