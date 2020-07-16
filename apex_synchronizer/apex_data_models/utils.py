from string import punctuation
import re

BASE_URL = 'https://api.apexvs.com/'
APEX_DATETIME_FORMAT = '%a, %d %b %Y %H:%M:%S %Z'
PS_DATETIME_FORMAT = '%Y/%m/%d'
PUNC_REGEX = re.compile(fr'[{punctuation + " "}]')
APEX_EMAIL_REGEX = re.compile("^[a-zA-Z0-9.!#$%&'*+\/=?^_`{|}~-]+@[a-zA-Z0-9]"
                              "(?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}"
                              "[a-zA-Z0-9])?)+$|^$/]")


def make_userid(first_name: str, last_name: str):
    """Makes a UserId from first and last names."""
    userid = re.sub(PUNC_REGEX, '', last_name.lower())[:4]
    userid += re.sub(PUNC_REGEX, '', first_name.lower())[:4]
    return userid
