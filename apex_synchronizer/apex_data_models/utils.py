from enum import Enum
from string import punctuation
from typing import Union
import re

import requests

from ..apex_session import TokenType
from ..exceptions import ApexAuthenticationError

BASE_URL = 'https://api.apexvs.com/'
APEX_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'  # Apex date format
# PowerSchool date format; how ApexDataObjects store dates
PS_DATETIME_FORMAT = '%Y/%m/%d'
PS_OUTPUT_FORMAT = '%Y-%m-%d'  # How dates are return from PowerQueries
PUNC_REGEX = re.compile(fr'[{punctuation + " "}]')
APEX_EMAIL_REGEX = re.compile("^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:.[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+$|^$/]")
# How many seconds to wait for a batch to finish processing
# before moving on
MAX_BATCH_WAIT_TIME = 180


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
    DuplicateUser = 3
    Unrecognized = 4

    __post_error_map = {
        "User doesn't exist": UserDoesNotExist,
        'No available Order': NotAvailableOrder,
        'Duplicate user': DuplicateUser,
        'User already exist': DuplicateUser
    }

    @classmethod
    def get_for_message(cls, msg: Union[str, int],
                        case_insensitive: bool = True) -> 'PostErrors':
        """
        This method is meant to be used to determine if a given message
        contains the key phrases that mark it as a specific error.
        If it can't find an associated error, it returns the
        `Unrecognized` PostError enum.

        :param msg: a given error message
        :param case_insensitive: whether the match should be case
            insensitive
        :return: a post error enum matching what was found in the msg
        """
        if case_insensitive:
            msg = msg.lower()
        for error_msg, post_error in cls.__post_error_map.value.items():
            if case_insensitive:
                error_msg = error_msg.lower()
            if error_msg in msg:
                return cls(post_error)

        return cls.Unrecognized

