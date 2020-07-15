import logging
import requests
from .apex_session import ApexSession
from .apex_data_models import ApexStudent
from collections import KeysView
from .enrollment import ApexEnrollment, PSEnrollment
from .ps_agent import fetch_students
from .exceptions import ApexNoEmailException
from typing import Collection, List, Set, Union


class ApexSynchronizer(object):

    def __init__(self):
        self.session = ApexSession()
        self.logger = logging.getLogger(__name__)
        # self.init_enrollment()

    def init_enrollment(self):
        if self._has_enrollment():
            return

        self.ps_enroll = PSEnrollment()
        self.logger.info('Retrieved enrollment info from PowerSchool.')
        self.apex_enroll = ApexEnrollment(access_token=self.session.access_token)
        self.logger.info('Retrieved enrollment info from Apex.')

    def sync_rosters(self):
        self.logger.info('Beginning roster synchronization.')

        self.logger.info('Comparing enrollment information.')
        to_enroll = self.ps_roster - self.apex_roster
        to_withdraw = self.apex_roster - self.ps_roster
        if len(to_enroll) == len(to_withdraw) == 0:
            self.logger.info('Rosters already in sync')
            return

        if len(to_enroll) > 0:
            self.enroll_students(set(to_enroll))
        else:
            self.logger.info('PowerSchool roster agrees with Apex roster.')

        if len(to_withdraw) > 0:
            self.logger.info(f'Found {len(to_withdraw)} students in Apex not enrolled in PowerSchool.')
            student: ApexStudent
            for i, student in enumerate(to_withdraw):
                progress = f':{i + 1}/{len(to_withdraw)}:'
                self.logger.info(f'{progress}Removing student {student} from Apex.')
                r = student.delete_from_apex(token=self.session.access_token)
                self.logger.debug(f'Received status from delete request: {r.status_code}')
        else:
            self.logger.info('Apex roster agrees with PowerSchool')

    def enroll_students(self, student_ids: Collection[int]):
        self.logger.info(f'Found {len(student_ids)} students in PowerSchool not enrolled in Apex.')
        token = self.session.access_token
        ps_json = fetch_students()
        apex_students = []
        seen_eduids = set()
        for obj in ps_json:
            eduid = obj['tables']['students']['eduid']
            if not eduid:
                self.logger.info('Student does not have an EDUID. Skipping...')
                continue

            eduid = int(eduid)
            if eduid in student_ids:
                if eduid in seen_eduids:
                    self.logger.debug(f'Duplicate EDUID \"{eduid}\". Skipping...')
                    continue
                try:
                    self.logger.info(f'Creating student for EDUID {eduid}')
                    apex_student = ApexStudent.from_powerschool(obj)
                    seen_eduids.add(eduid)
                    apex_students.append(apex_student)
                except ApexNoEmailException:
                    self.logger.info(f'Student with EDUID "{eduid}" has no email. Skipping...')

        if len(apex_students) > 0:
            self.logger.info(f'Posting {len(apex_students)} students.')
            r = ApexStudent.post_batch(token, apex_students)
            try:
                r.raise_for_status()
                self.logger.debug('Received status code ' + str(r.status_code))
            except requests.exceptions.HTTPError:
                self.logger.exception('Failed to POST students. Received status ' + str(r.status_code))

                as_json = r.json()
                if type(as_json) is dict:
                    self.logger.info('Found duplicates.')
                    put_duplicates(as_json, apex_students, token)
                elif type(as_json) is list:
                    self.logger.info('Removing invalid entries.')
                    repost_students(as_json, apex_students, token)
                else:
                    self.logger.exception('Response text:\n' + r.text)

    def _has_enrollment(self):
        return hasattr(self, 'ps_enroll') and hasattr(self, 'apex_enroll')

    @property
    def apex_roster(self) -> Union[Set[int], KeysView]:
        """Avoids creating an ApexEnrollment object if it doesn't have to."""
        try:
            return self.apex_enroll.roster
        except AttributeError:
            try:
                return self._apex_roster
            except AttributeError:
                token = self.session.access_token
                self._apex_roster = set(ApexStudent.get_all(token=token, ids_only=True))
                return self._apex_roster

    @property
    def ps_roster(self) -> KeysView:
        """Exists only to mirror the `apex_roster` method."""
        try:
            return self.ps_enroll.roster
        except AttributeError:
            self.ps_enroll = PSEnrollment()
            return self.ps_enroll.roster


def put_duplicates(json_obj: dict, apex_students: List[ApexStudent],token: str):
    logger = logging.getLogger(__name__)
    if not json_obj['HasError']:
        return

    duplicates = [user['Index'] for user in json_obj['studentUsers']]
    for student_idx in duplicates:
        student = apex_students[student_idx]
        logger.info(f'Putting student with EDUID {student.import_user_id}.')
        student.put_to_apex(token)


def repost_students(json_obj: dict, apex_students: List[ApexStudent], token: str):
    logger = logging.getLogger(__name__)
    to_retry = []
    for entry in json_obj:
        if entry['ValidationError']:
            logger.info(f'Student with EDUID {entry["ImportUserId"]} did not pass validation.')
        else:
            to_retry.append(apex_students[entry['Index']])

    if to_retry:
        logger.info(f'Attempting to POST remaining {len(to_retry)} students.')
        r = ApexStudent.post_batch(token, to_retry)
        try:
            r.raise_for_status()
            logger.info('Successfully POSTed remaining students.')
        except requests.exceptions.HTTPError:
            logger.exception(f'Failed to post {len(to_retry)} students. Received response:\n'
                             + r.text)
    else:
        logger.info('No entries passed validation.')
