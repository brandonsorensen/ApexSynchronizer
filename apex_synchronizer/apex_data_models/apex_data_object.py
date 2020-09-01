from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from time import sleep
from typing import Collection, Dict, List, Tuple, Union
from urllib.parse import urljoin
import json
import logging
import re

from requests import Response
import requests

from . import utils as adm_utils
from .page_walker import PageWalker
from .utils import check_args, SCHOOL_CODE_MAP
from .. import exceptions, utils
from ..apex_session import ApexSession, TokenType
from ..utils import get_header


class ApexDataObject(ABC):

    """
    The base class from which :class:`ApexStaffMember`,
    :class:`ApexStudent` and :class:`ApexClassroom` will inherit.
    Defines a number of class methods common to all objects that aid in
    making RESTful calls to the Apex API. Additionally, contains a
    number of abstract methods that must be implemented by the
    subclasses.
    """

    main_id = 'ImportUserId'
    max_batch_size = 2500

    def __init__(self, import_user_id: Union[str, int],
                 import_org_id: int):
        """Initializes instance variables."""
        self.import_user_id = import_user_id
        if not import_user_id:
            raise exceptions.NoUserIdException()
        try:
            self.import_org_id = int(import_org_id)
        except ValueError as ve:
            raise exceptions.ApexUnrecognizedOrganizationError(import_org_id) \
                from ve
        if self.import_org_id not in SCHOOL_CODE_MAP.keys():
            raise exceptions.ApexUnrecognizedOrganizationError(import_org_id)

    def __eq__(self, other: 'ApexDataObject'):
        return (isinstance(other, ApexDataObject)
                and self.to_json() == other.to_json())

    @classmethod
    def get(cls, import_id: Union[str, int], token: TokenType = None,
            session: requests.Session = None) -> 'ApexDataObject':
        """
        Gets the ApexDataObject corresponding to a given ImportId.

        :param token: ApexAccessToken
        :param session: an exising Apex session
        :param import_id: the ImportId of the object
        :return: an ApexDataObject corresponding to the given ImportId
        """
        r = cls._get_response(str(import_id), token=token,
                              session=session)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            if r.status_code == 401:
                raise exceptions.ApexNotAuthorizedError()
            raise exceptions.ApexObjectNotFoundException(import_id)
        except requests.exceptions.ConnectionError:
            raise exceptions.ApexConnectionException()

        try:
            return cls._parse_get_response(r)
        except KeyError:
            raise exceptions.ApexIncompleteDataException()

    @classmethod
    @abstractmethod
    def _parse_get_response(cls, r: Response) -> 'ApexDataObject':
        """
        A helper method for the `get` method. Parses the JSON object
        returned by the `_get_response`, validates it, and returns an
        instance of the corresponding class.

        :param Response r: the reponse returned by the `_get_response`
            method.
        :return: an instance of type `cls` corresponding to the JSON
            object in `r`.
        """
        pass

    @classmethod
    def _get_response(cls, import_id, token: TokenType = None,
                      session: requests.Session = None) -> Response:
        """
        Calls a GET operation for a given ImportId and returns the
        response. The first (and constant across all subclasses)
        component of the `get` method.

        :param token: the Apex access token
        :param import_id:
        :return: the response from the GET operation
        """
        agent = check_args(token, session)
        url = urljoin(cls.url + '/', import_id)
        if isinstance(agent, ApexSession):
            token = agent.access_token
        custom_args = {'importUserId': import_id}
        if type(agent) is requests.Session:
            agent.headers.update(custom_args)
            r = agent.get(url=url)
        else:
            header = get_header(token, custom_args)
            r = agent.get(url=url, headers=header)
        return r

    @classmethod
    def get_all(cls, token: TokenType = None, archived: bool = False,
                session: requests.Session = None) -> List['ApexDataObject']:
        """
        Gets all objects of type `cls` in the Apex database.

        :param token: Apex access token
        :param session: an existing Apex session
        :param bool ids_only: Whether to only return IDs
        :param archived: whether or not to return archived objects
        :return: a list containing all objects of this type in the Apex
            database
        """
        return cls._get_all(token=token, archived=archived, session=session,
                            ids_only=False)

    @classmethod
    def get_all_ids(cls, token: TokenType = None, archived: bool = False,
                    session: requests.Session = None) -> List[Union[int, str]]:
        """
        Gets all objects of type `cls` in the Apex database.

        :param token: Apex access token
        :param session: an existing Apex session
        :param bool ids_only: Whether to only return IDs
        :param archived: whether or not to return archived objects
        :return: a list containing all objects of this type in the Apex
            database
        """
        return cls._get_all(token=token, archived=archived, session=session,
                            ids_only=True)

    @classmethod
    def _get_all(cls, token: TokenType, archived: bool,
                 session: requests.Session, ids_only: bool) \
            -> List[Union['ApexDataObject', str, int]]:
        """
        Gets all objects of type `cls` in the Apex database.

        :param token: Apex access token
        :param session: an existing Apex session
        :param bool ids_only: Whether to only return IDs
        :param archived: whether or not to return archived objects
        :return: a list containing all objects of this type in the Apex
            database
        """
        logger = logging.getLogger(__name__)
        ret_val = []
        walker = PageWalker(logger=logger, session=session)

        for current_page, r in enumerate(walker.walk(cls.url, token=token)):
            cls._parse_response_page(token=token, json_objs=r.json(),
                                     page_number=current_page, session=session,
                                     all_objs=ret_val, archived=archived,
                                     ids_only=ids_only)

        return ret_val

    def post_to_apex(self, token: TokenType = None,
                     session: requests.Session = None) -> Response:
        """
        Posts the information contained in this object to the Apex API.
        Simply a convenience method that passes this object to the
        `post_batch` class method.

        :param token: Apex access token
        :param session: existing requests Session object
        :return: the response returned by the POST operation
        """
        return self.post_batch([self], token=token, session=session)

    @classmethod
    def post_batch(cls, objects: Collection['ApexDataObject'],
                   token: TokenType = None,
                   session: requests.Session = None):
        """
        Posts a batch of `ApexDataObjects` to the Apex API. The `object`
        parameter
        must be heterogeneous, i.e. must contain objects of all the
        same type. Attempting to post an object not of the correct
        subclass (i.e., attempting to call `ApexStudent.post_batch`
        with even one `ApexStaffMember` will result in an error.

        Must supply one of either `token` or `session`. If both are
        supplied, `session` will take precedence.

        :param objects: a heterogeneous collection of `ApexDataObjects`
        :param token: Apex access token
        :param session: an existing requests session
        :return: the result of the POST operation
        """
        logger = logging.getLogger(__name__)
        if len(objects) > cls.max_batch_size:
            raise exceptions.ApexMaxPostSizeException(max_size=cls.max_batch_size)

        agent = check_args(token, session)
        url = cls.url if len(objects) <= 50 else urljoin(cls.url + '/', 'batch')
        payload = json.dumps({cls.post_heading: [c.to_json() for c in objects]})

        header = get_header(token) if not isinstance(agent, requests.Session) \
            else None

        r = agent.post(url=url, data=payload, headers=header)

        # In the event that a batch_token is returned, we want to
        # a reasonable amount of time for it to complete.
        try:
            status_token = r.json()['BatchStatusToken']
            assert len(objects) > 50
            r = cls.check_batch_status(status_token, session=session)
            msg = r.json()['Message']
            logger.debug(f'Received status token {status_token}.')
            expire_at = (datetime.now()
                         + timedelta(seconds=adm_utils.MAX_BATCH_WAIT_TIME))
            processing = msg.lower().startswith('the batch processing results')
            logger.info('Received batch status token ' + str(status_token))

            while processing and datetime.now() < expire_at:
                sleep(1)
                logger.debug('Batch still processing...')
                logger.debug(f'{expire_at - datetime.now()} seconds remaining...')
                logger.debug('Received JSON response:\n' + msg)
                r = cls.check_batch_status(status_token, session=session)
                msg = r.json()['Message']
                processing = (msg.lower()
                              .startswith('the batch processing results'))
            if processing:
                raise exceptions.ApexBatchTimeoutError(status_token)

            return r

        except KeyError as e:
            if e.args[0] != 'BatchStatusToken':
                raise e

        return r

    def delete_from_apex(self, token: TokenType = None,
                         session: requests.Session = None) -> Response:
        """
        Deletes this object from the Apex database

        :param token: Apex access token
        :param session: an existing Apex session
        :return: the response from the DELETE operation
        """
        agent = check_args(token, session)
        custom_args = {
            'importUserId': str(self.import_user_id),
            'orgId': str(self.import_org_id)
        }
        url = urljoin(self.url + '/', str(self.import_user_id))
        if isinstance(agent, requests.Session):
            agent.headers.update(custom_args)
            r = agent.delete(url=url)
        else:
            header = get_header(token, custom_args)
            r = agent.delete(url=url, headers=header)
        return r

    def put_to_apex(self, token: TokenType = None,
                    session: requests.Session = None) -> Response:
        """
        Useful for updating a record in the Apex database.

        :param token: Apex access token
        :param session: an existing Apex session
        :param main_id: the idenitifying class attribute: ImportUserId
            for `ApexStudent` and `ApexStaffMember` objects,
            ImportClassroomId for `ApexClassroom` objects
        :return: the response from the PUT operation.
        """
        agent = check_args(token, session)
        main_to_snake = utils.camel_to_snake(self.main_id)
        url = urljoin(self.url + '/', str(getattr(self, main_to_snake)))
        if not isinstance(session, requests.Session):
            header = get_header(token)
        else:
            header = None
        payload = self.get_put_payload()
        r = agent.put(url=url, headers=header, data=json.dumps(payload))
        return r

    def get_put_payload(self) -> dict:
        """
        A helper method for getting the PUT payload. This exists to
        accommodate `ApexClassroom` object, who need one extra step
        compared to the others.
        """
        payload = self.to_json()
        del payload[self.main_id]  # Given in the URL
        # We don't want to update a password
        for key in ('LoginPw',):
            if key in payload.keys():
                del payload[key]

        return payload

    @property
    @abstractmethod
    def url(self) -> str:
        """The class's base URL."""
        pass

    @property
    @abstractmethod
    def post_heading(self) -> str:
        """The heading required for a POST call."""
        pass

    @property
    @abstractmethod
    def role(self) -> str:
        """The role of a given class, either T or S"""
        pass

    @property
    @abstractmethod
    def ps2apex_field_map(self) -> dict:
        """
        A mapping from field names return by PowerSchool queries to
        their respective Apex JSON fields for each class.
        """
        pass

    @classmethod
    @abstractmethod
    def from_powerschool(cls, json_obj: dict, already_flat: bool = False) \
            -> 'ApexDataObject':
        """
        Creates an instance of the class from a JSON object returned
        from PowerSchool.

        :param dict json_obj: the PowerSchool JSON object
        :param bool already_flat: Whether the JSON object has already
            been flattened
        :return: an instance of type cls representing the JSON object
        """
        pass

    @classmethod
    def _init_kwargs_from_ps(cls, json_obj, already_flat=False):
        """
        A helper method for the `from_powerschool` method. Takes the
        PowerSchool JSON and transforms it according to
        `ps2apex_field_map` mappings.

        :param json_obj: the PowerSchool JSON object
        :return: the same JSON object with transformed keys.
        """
        kwargs = {}
        if not already_flat:
            json_obj = utils.flatten_ps_json(json_obj)
        for ps_key, apex_key in cls.ps2apex_field_map.items():
            if type(apex_key) is str:
                kwargs[apex_key] = json_obj[ps_key]
            else:
                for k in apex_key:
                    kwargs[k] = json_obj[ps_key]
        return kwargs

    @classmethod
    def _init_kwargs_from_get(cls, r: Response) -> Tuple[dict, dict]:
        """
        Helper method for the `get` method. Converts the keys from a
        GET response JSON object into the proper style for initializing
        ApexDataObject objects.

        :param r: the response of a GET call
        :return: a Tuple of the converted mappings and the original
            JSON response
        """
        json_obj = json.loads(r.text)
        kwargs = {}
        params = set(cls.ps2apex_field_map.values())
        for key, value in json_obj.items():
            snake_key = utils.camel_to_snake(key)
            if snake_key in params:
                kwargs[snake_key] = value

        return kwargs, json_obj

    @classmethod
    def _parse_response_page(cls, json_objs: List[dict], page_number: int,
                             all_objs: List[Union['ApexDataObject', int]],
                             archived: bool = False, ids_only: bool = False,
                             token: TokenType = None,
                             session: requests.Session = None):
        """
        Parses a single page of a GET response and populates the
        `all_objs` list with either `ApexDataObject` objects or their
        ImportUserIds depending on the value of `ids_only`. Returned
        only ImportUserIds is far more efficient as returning the the
        objects requires making GET calls for each objects whereas the
        IDs are given in a single (paginated) call .

        :param token: Apex access token
        :param json_objs: the objects in the response page
        :param all_objs: the global list of all objects collected
            thus far
        :param archived: whether to return archived objects
        :param ids_only: whether to return on the IDs
        :return: a list of all objects or their IDs.
        """
        logger = logging.getLogger(__name__)
        for i, obj in enumerate(json_objs):
            progress = f'page {int(page_number) + 1}:{i + 1}/{len(json_objs)}' \
                       f':total {len(all_objs) + 1}'
            try:
                if not archived:
                    try:
                        if obj['RoleStatus'] == 'Archived':
                            continue  # Don't return archived
                    except KeyError:
                        pass
                iuid = obj[cls.main_id]
                if issubclass(cls, ApexNumericId):
                    try:
                        iuid = int(iuid)
                    except TypeError:
                        logger.debug(f'Object with ID "{iuid}" cannot be '
                                     'created because its ID is invalid.')
                        continue

                if not iuid:
                    logger.info('Object has no ImportUserId. Skipping...')
                    continue

                if ids_only:
                    logger.info(f'{progress}:Adding ImportUserId {iuid}.')
                    all_objs.append(iuid)
                else:
                    logger.info(f'{progress}:Creating {cls.__name__} '
                                f'with ImportUserId {iuid}')
                    apex_obj = cls.get(import_id=iuid, token=token,
                                       session=session)
                    all_objs.append(apex_obj)
            except exceptions.ApexObjectNotFoundException:
                error_msg = f'Could not retrieve object of type {cls.__name__} \
                            bearing ImportID {obj[cls.main_id]}. Skipping object'
                logger.info(error_msg)
            except exceptions.ApexMalformedEmailException as e:
                logger.info(e)
            except exceptions.ApexIncompleteDataException as e:
                logger.debug(e)
            except exceptions.ApexError:
                logger.exception('Received Apex error:')

    @classmethod
    def check_batch_status(cls, status_token: Union[str, int],
                           access_token: TokenType = None,
                           session: requests.Session = None) -> Response:
        agent = check_args(token=access_token, session=session)
        url = urljoin(cls.url + '/', 'batch/')
        url = urljoin(url, str(status_token))
        if not isinstance(agent, requests.Session):
            header = get_header(token=access_token)
        else:
            header = None
        return agent.get(url=url, headers=header)

    @classmethod
    def parse_batch(cls, batch: Response) -> Dict[str, adm_utils.PostErrors]:
        errors = {}
        as_json = batch.json()

        # Sometimes the API labels the JSON array with the specific
        # class label and other times it simply uses "User"
        array_label = None

        for label in cls.post_heading, 'Users':
            if label in as_json:
                array_label = label
                break

        if array_label is None:
            raise exceptions.ApexMalformedJsonException(as_json)

        for obj in as_json[array_label]:
            if obj['Code'] == 200:
                continue
            msg = obj['Message']
            user_id = obj['ImportUserId']

            errors[user_id] = adm_utils.PostErrors.get_for_message(msg)

        return errors

    def to_dict(self) -> dict:
        """Converts attributes to a dictionary."""
        return self.__dict__

    def to_json(self) -> dict:
        """
        Converts instance attributes to dictionary and modifies their
        contents to prepare them for submission to the Apex API.
        """
        json_obj = {}
        for key, value in self.to_dict().items():
            if value is None:
                value = 'null'
            if isinstance(value, int):
                value = str(value)
            json_obj[utils.snake_to_camel(key)] = value
        json_obj['Role'] = self.role
        return json_obj

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return f'{self.__class__.__name__}({str(self)})'

    def __hash__(self):
        return hash((self.import_user_id,
                     self.import_org_id,
                     self.__class__.__name__))


class ApexNumericId(ApexDataObject, ABC):

    def __init__(self, import_user_id: int, import_org_id: int):
        super().__init__(import_user_id=int(import_user_id),
                         import_org_id=int(import_org_id))

    def _parse_response_page(cls, json_objs: List[dict], page_number: int,
                             all_objs: List[Union['ApexDataObject', int]],
                             archived: bool = False, ids_only: bool = False,
                             token: TokenType = None,
                             session: requests.Session = None):
        super(ApexDataObject)._parse_response_page(
            json_objs=json_objs, page_number=page_number, all_objs=all_objs,
            archived=archived, ids_only=ids_only, token=token, session=session
        )
        if ids_only:
            all_objs[:] = [int(id_) for id_ in all_objs]


class ApexUser(ApexDataObject, ABC):

    """
    An Apex object that represents a person, i.e., not a classroom.
    Cannot be instantiated. Initializes the attributes common to
    :class:`ApexStaffMember` and :class:`ApexStudent` and
    provides a property method for returning the first and last
    names combined into a single string. Otherwise, serves a chiefly
    semantic purpose.
    """

    def __init__(self, import_user_id: Union[int, str],
                 import_org_id: int, first_name: str,
                 middle_name: str, last_name: str, email: str,
                 login_id: str):
        self.first_name = first_name.strip() if first_name else first_name
        self.middle_name = middle_name.strip() if middle_name else middle_name
        self.last_name = last_name.strip() if last_name else last_name
        if not email:
            raise exceptions.ApexNoEmailException(self.first_last)
        email = email.strip()
        try:
            super().__init__(import_user_id, import_org_id)
        except exceptions.NoUserIdException:
            raise exceptions.ApexNoEmailException(self.first_last)

        if not re.match(adm_utils.APEX_EMAIL_REGEX, email):
            raise exceptions.ApexMalformedEmailException(self.import_user_id,
                                                         email)
        self.email = email
        self.login_id = login_id

    @property
    def first_last(self) -> str:
        """
        Returns the first and last name combined, separated by a space.
        """
        return self.first_name + ' ' + self.last_name
