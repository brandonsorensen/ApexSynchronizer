from enum import Enum
from string import punctuation
import re

import requests

from ..apex_session import TokenType
from ..exceptions import ApexAuthenticationError

BASE_URL = 'https://api.apexvs.com/'
APEX_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
PS_DATETIME_FORMAT = '%Y/%m/%d'
PUNC_REGEX = re.compile(fr'[{punctuation + " "}]')
APEX_EMAIL_REGEX = re.compile("^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:.[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$|^$/]")
# How many seconds to wait for a batch to finish processing
# before moving on
MAX_BATCH_WAIT_TIME = 90


def make_userid(first_name: str, last_name: str):
    """Makes a UserId from first and last names."""
    userid = re.sub(PUNC_REGEX, '', last_name.lower())[:4]
    userid += re.sub(PUNC_REGEX, '', first_name.lower())[:4]
    return userid


def check_args(token: TokenType, session: requests.Session):
    """Throws on error if both are not truthy.."""
    if not any((token, session)):
        raise ApexAuthenticationError('Must supply one of either `token` '
                                      'or `session`.')

    return session if session else requests


class PostErrors(Enum):
    NotAvailableOrder = 1
    UserDoesNotExist = 2
    Unrecognized = 3


post_error_map = {
    "User doesn't exist": PostErrors.UserDoesNotExist,
    'No available Order': PostErrors.NotAvailableOrder
}

