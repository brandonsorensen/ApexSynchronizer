from typing import Generator
import logging

import requests

from ..apex_session import TokenType
from ..exceptions import ApexAuthenticationError
from ..utils import get_header


class PageWalker(object):
    """
    A class designed to abstract the process of walking over multiple
    pagified responses.
    """
    def __init__(self, logger: logging.Logger = None,
                 session: requests.Session = None):
        """
        Provide an optional custom logger or existing requests session

        :param logger: custom logger
        :param session: existing requests session
        """
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.session = session

    def walk(self, url: str, token: TokenType = None,
             custom_args: dict = None) \
            -> Generator[requests.Response, None, None]:
        """
        A generator for walking over the pagified response JSON objects.

        :param url: the URL to walk
        :param token: an Apex access token, ignored if self.session
            is set
        :param custom_args: any custom arguments to pass to the request
            header
        """
        if custom_args is None:
            custom_args = {}

        close_session = self.session is None
        if self.session is None:
            self.session = requests.Session()
            if token is None:
                raise ApexAuthenticationError('No token provided and `session` '
                                              'attribute is not set.')
            header = get_header(token, custom_args=custom_args)
        else:
            header = custom_args
        self.session.headers.update(header)

        current_page = 1
        r = self.session.get(url=url)
        try:
            r.raise_for_status()
            total_pages = int(r.headers['total-pages'])
        except (KeyError, requests.exceptions.HTTPError):
            # Don't want to error handle here, so an error status
            # is returned
            if close_session:
                self.session.close()
            yield r
            raise StopIteration
        while current_page <= total_pages:
            self.logger.info(f'Reading page {current_page}/{total_pages} '
                             'of get_all response.')
            yield r
            current_page += 1
            self.session.headers['page'] = str(current_page)

            if current_page <= total_pages:
                r = self.session.get(url=url)

        if close_session:
            self.session.close()
