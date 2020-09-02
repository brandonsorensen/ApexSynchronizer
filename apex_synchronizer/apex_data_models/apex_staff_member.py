from typing import List, Union
from urllib.parse import urljoin

from requests import Session
import requests

from .apex_data_object import ApexUser
from .utils import BASE_URL, TokenType
from .. import exceptions


class ApexStaffMember(ApexUser):

    """
    Represents a staff member (likely a teacher) in the Apex database.

    :param Union[int, str] import_user_id:
                            identifier for the database, common to
                            Apex and PowerSchool
    :param Union[int, str] import_org_id: the school to which teacher
                                          staff member belongs
    :param str first_name: the staff member's first/given name
    :param str middle_name: the staff member's middle name
    :param str last_name: the staff member's last/surname
    :param str email: the staff member's school email address (optional)
    """

    post_heading = 'staffUsers'
    ps2apex_field_map = {
        'school_id': 'import_org_id',
        'email': 'email',
        'first_name': 'first_name',
        'middle_name': 'middle_name',
        'last_name': 'last_name'
    }
    url = urljoin(BASE_URL, 'staff')
    role = 'T'
    role_set = {'M', 'T', 'TC', 'SC'}
    """
    m = mentor
    t = teacher
    tc = technical coordinator
    sc = site_coordinator
    """

    def __init__(self, import_org_id: int, first_name: str,
                 middle_name: str, last_name: str, email: str):
        email_lower = email.lower().strip() if email else None
        if middle_name == 'null':
            middle_name = None
        super().__init__(
            import_user_id=email_lower, import_org_id=import_org_id,
            first_name=first_name, middle_name=middle_name,
            last_name=last_name, email=email,
            login_id=email_lower.split('@')[0] if email else None
        )

    def get_classrooms(self, token) -> List['ApexClassroom']:
        # TODO
        pass

    @classmethod
    def get_all_orgs(cls, import_id: str, token: TokenType = None,
                      session: Session = None) -> List['ApexStaffMember']:
        """
        Exactly the same as the `get` method with the difference that
        if a staff member belongs to multiple organizations, this
        method will return a new `ApexStaffMember` object for each
        organization.

        :param token: Apex access token
        :param session: an existing Apex session
        :param import_id: the ImportId of the object
        :return:
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
            kwargs, json_obj = cls._init_kwargs_from_get(r)
            objs = []
            for org in json_obj['Organizations']:
                kwargs['import_org_id'] = org['ImportOrgId']
                objs.append(cls(**kwargs))
        except KeyError:
            raise exceptions.ApexIncompleteDataException()

        return objs

    @classmethod
    def from_powerschool(cls, json_obj, already_flat: bool = False) \
            -> 'ApexStaffMember':
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj,
                                          already_flat=already_flat)
        try:
            # In case of old version of PowerSchool query
            del kwargs['login_id']
        except KeyError:
            pass

        return cls(**kwargs)

    @classmethod
    def _parse_get_response(cls, r) -> 'ApexStaffMember':
        kwargs, json_obj = cls._init_kwargs_from_get(r)
        orgs = json_obj['Organizations']
        kwargs['import_org_id'] = orgs[0]['ImportOrgId']
        return cls(**kwargs)


