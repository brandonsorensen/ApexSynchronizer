from abc import ABC, abstractmethod
from typing import Collection, List, Tuple, Union
from urllib.parse import urljoin
import json
import logging

from requests import Response
import requests

from .. import exceptions, utils
from ..utils import get_header


class ApexDataObject(ABC):

    """
    The base class from which `ApexStaffMember`, 'ApexStudent` and
    `ApexClassroom` will inherit. Defines a number of class methods
    common to all objects that aid in making RESTful calls to the Apex
    API. Additionally, contains a number of abstract methods that must
    be implemented by the subclasses.
    """

    def __init__(self, import_user_id, import_org_id):
        """Initializes instance variables."""
        self.import_user_id = import_user_id
        if not import_user_id:
            raise exceptions.NoUserIdException
        self.import_org_id = import_org_id

    @classmethod
    def get(cls, token, import_id: Union[str, int]) -> 'ApexDataObject':
        """
        Gets the ApexDataObject corresponding to a given ImportId.

        :param token: ApexAccessToken
        :param import_id: the ImportId of the object
        :return: an ApexDataObject corresponding to the given ImportId
        """
        try:
            r = cls._get_response(token, str(import_id))
            r.raise_for_status()
        except requests.exceptions.HTTPError:
            raise exceptions.ApexObjectNotFoundException(import_id)
        except requests.exceptions.ConnectionError:
            raise exceptions.ApexConnectionException()

        return cls._parse_get_response(r)

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
    def _get_response(cls, token: str, import_id) -> Response:
        """
        Calls a GET operation for a given ImportId and returns the
        response. The first (and constant across all subclasses)
        component of the `get` method.

        :param token: the Apex access token
        :param import_id:
        :return: the response from the GET operation
        """
        custom_args = {
            'importUserId': import_id
        }
        header = get_header(token, custom_args)
        url = urljoin(cls.url + '/', import_id)
        r = requests.get(url=url, headers=header)
        return r

    @classmethod
    def get_all(cls, token, ids_only=False, archived=False) \
            -> List[Union['ApexDataObject', int]]:
        """
        Gets all objects of type `cls` in the Apex database.

        :param token: Apex access token
        :param archived: whether or not to return archived objects
        :return: a list containing all objects of this type in the Apex
            database
        """
        logger = logging.getLogger(__name__)

        current_page = 1
        ret_val = []

        header = get_header(token)
        r = requests.get(url=cls.url, headers=header)
        total_pages = int(r.headers['total-pages'])
        while current_page <= total_pages:
            logger.info(f'Reading page {current_page}/{total_pages} of '
                        'get_all response.')
            cls._parse_response_page(token=token, json_objs=r.json(),
                                     page_number=current_page, all_objs=ret_val,
                                     archived=archived, ids_only=ids_only)
            current_page += 1
            header['page'] = str(current_page)

            if current_page <= total_pages:
                r = requests.get(url=cls.url, headers=header)

        return ret_val

    def post_to_apex(self, token) -> Response:
        """
        Posts the information contained in this object to the Apex API.
        Simply a convenience method that passes this object to the
        `post_batch` class method.

        :param token: Apex access token
        :return: the response returned by the POST operation
        """
        return self.post_batch(token, [self])

    @classmethod
    def post_batch(cls, token: str, objects: Collection['ApexDataObject']):
        """
        Posts a batch of `ApexDataObjects` to the Apex API. The `object`
        parameter
        must be heterogeneous, i.e. must contain objects of all the
        same type. Attempting to post an object not of the correct
        subclass (i.e., attempting to call `ApexStudent.post_batch`
        with even one `ApexStaffMember` will result in an error.

        :param token: Apex access token
        :param objects: a heterogeneous collection of `ApexDataObjects`
        :return: the result of the POST operation
        """
        header = get_header(token)
        payload = json.dumps({cls.post_heading: [c.to_json() for c in objects]})
        url = cls.url if len(objects) <= 50 else urljoin(cls.url + '/', 'batch')
        r = requests.post(url=url, data=payload, headers=header)
        return r

    def delete_from_apex(self, token) -> Response:
        """
        Deletes this object from the Apex database

        :param token: Apex access token
        :return: the response from the DELETE operation
        """
        custom_args = {
            'importUserId': self.import_user_id,
            'orgId': self.import_org_id
        }
        header = get_header(token, custom_args)
        url = urljoin(self.url + '/', self.import_user_id)
        r = requests.delete(url=url, headers=header)
        return r

    def put_to_apex(self, token, main_id='ImportUserId') -> Response:
        """
        Useful for updating a record in the Apex database.

        :param token: Apex access token
        :param main_id: the idenitifying class attribute: ImportUserId
            for `ApexStudent` and `ApexStaffMember` objects,
            ImportClassroomId for `ApexClassroom` objects
        :return: the response from the PUT operation.
        """
        header = get_header(token)
        url = urljoin(self.url + '/', self.import_user_id)
        payload = self.to_json()
        del payload[main_id]  # Given in the URL
        # We don't want to update a password
        if 'LoginPw' in payload.keys():
            del payload['LoginPw']
        r = requests.put(url=url, headers=header, data=payload)
        return r

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
    def from_powerschool(cls, json_obj: dict) -> 'ApexDataObject':
        """
        Creates an instance of the class from a JSON object returned
        from PowerSchool.

        :param json_obj: the PowerSchool JSON object
        :return: an instance of type cls representing the JSON object
        """
        pass

    @classmethod
    def _init_kwargs_from_ps(cls, json_obj):
        """
        A helper method for the `from_powerschool` method. Takes the
        PowerSchool JSON and transforms it according to
        `ps2apex_field_map` mappings.

        :param json_obj: the PowerSchool JSON object
        :return: the same JSON object with transformed keys.
        """
        kwargs = {}
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
    def _parse_response_page(cls, token: str, json_objs: List[dict], page_number: float,
                             all_objs: List[Union['ApexDataObject', int]],
                             archived: bool = False, ids_only: bool = False):
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
            progress = f'page {int(page_number)}:{i + 1}/{len(json_objs)}:total {len(all_objs) + 1}'
            try:
                if not archived and obj['RoleStatus'] == 'Archived':
                    continue  # Don't return archived
                iuid = obj['ImportUserId']
                if not iuid:
                    logger.info('Object has no ImportUserId. Skipping...')
                    continue

                if ids_only:
                    logger.info(f'{progress}:Adding ImportUserId {iuid}.')
                    all_objs.append(int(iuid))
                else:
                    logger.info(f'{progress}:Creating {cls.__name__} with ImportUserId {iuid}')
                    apex_obj = cls.get(token, import_id=iuid)
                    all_objs.append(apex_obj)
            except exceptions.ApexObjectNotFoundException:
                error_msg = f'Could not retrieve object of type {cls.__name__} \
                            bearing ImportID {obj["ImportUserID"]}. Skipping object'
                logger.info(error_msg)
            except exceptions.ApexMalformedEmailException as e:
                logger.info(e)
            except KeyError:
                pass

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
            json_obj[utils.snake_to_camel(key)] = value
        json_obj['Role'] = self.role
        return json_obj

    def __str__(self):
        return str(self.to_dict())

    def __repr__(self):
        return f'{self.__class__.__name__}({str(self)})'
