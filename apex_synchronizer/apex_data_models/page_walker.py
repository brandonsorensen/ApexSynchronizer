from typing import Generator
import logging

import requests

from ..apex_session import TokenType
from ..utils import get_header


class PageWalker(object):

    def __init__(self, logger: logging.Logger = None):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def walk(self, url: str, token: TokenType,
             session: requests.Session = None,
             custom_args: dict = None) \
            -> Generator[requests.Response, None, None]:
        if custom_args is None:
            custom_args = {}

        close_session = session is None
        if session is None:
            session = requests.Session()

        current_page = 1
        session.headers.update(get_header(token, custom_args=custom_args))

        r = session.get(url=url)
        total_pages = int(r.headers['total-pages'])
        while current_page <= total_pages:
            self.logger.info(f'Reading page {current_page}/{total_pages} '
                             'of get_all response.')
            yield r
            current_page += 1
            session.headers['page'] = str(current_page)

            if current_page <= total_pages:
                r = session.get(url=url)

        if close_session:
            session.close()
