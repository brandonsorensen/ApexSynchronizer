from typing import List, Union
from urllib.parse import urljoin
import re

from .apex_data_object import ApexDataObject
from .utils import BASE_URL, APEX_EMAIL_REGEX
from .. import exceptions


class ApexStaffMember(ApexDataObject):

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
    :param str login_id: the staff member's login ID
    """

    url = urljoin(BASE_URL, 'staff')
    role = 'T'
    role_set = {'M', 'T', 'TC', 'SC'}
    post_heading = 'staffUsers'
    """
    m = mentor
    t = teacher
    tc = technical coordinator
    sc = site_coordinator
    """

    ps2apex_field_map = {
        'school_id': 'import_org_id',
        'email': 'email',
        'first_name': 'first_name',
        'middle_name': 'middle_name',
        'last_name': 'last_name'
    }

    def __init__(self, import_org_id: Union[int, str], first_name: str,
                 middle_name: str, last_name: str, email: str):
        if not email:
            raise exceptions.ApexStaffNoEmailException(
                first_name + ' ' + last_name
            )
        if not re.match(APEX_EMAIL_REGEX, email):
            raise exceptions.ApexMalformedEmailException(email, email)

        email_lower = email.lower()
        super().__init__(email_lower, import_org_id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.login_id = email_lower.split('@')[0]

    @classmethod
    def from_powerschool(cls, json_obj) -> 'ApexStaffMember':
        kwargs = cls._init_kwargs_from_ps(json_obj=json_obj)
        try:
            # In case of old version of PowerSchool query
            del kwargs['login_id']
        except KeyError:
            pass

        return cls(**kwargs)

    def get_with_orgs(self, token) -> List['ApexStaffMember']:
        """
        Exactly the same as the `get` method with the difference that
        if a staff member belongs to multiple organizations, this
        method will return a new `ApexStaffMember` object for each
        organization.

        :param token: Apex access token
        :return:
        """
        # TODO
        pass

    @classmethod
    def _parse_get_response(cls, r) -> 'ApexStaffMember':
        # TODO
        print(r.text)

    def get_classrooms(self, token) -> List['ApexClassroom']:
        # TODO
        pass

    def to_json(self) -> dict:
        json_obj = super().to_json()
        if json_obj['Email'] == 'null':
            json_obj['Email'] = 'dummy@malad.us'
        return json_obj


