import logging
from collections import KeysView
from typing import Collection, List, Set, Union

import requests

from . import exceptions
from .apex_data_models import ApexStudent, ApexClassroom
from .apex_data_models.apex_classroom import walk_ps_sections
from .apex_session import ApexSession, TokenType
from .enrollment import ApexEnrollment, PSEnrollment
from .exceptions import ApexStudentNoEmailException, ApexMalformedEmailException
from .ps_agent import fetch_students


class ApexSynchronizer(object):

    """
    A driver class that synchronizes data between the PowerSchool and
    Apex databases. In general, it treats the PowerSchool database as
    a "master" copy that Apex data should match.

    :ivar ApexSession session: an open ApexSession object
    :ivar logging.Logger logger: a module-wide logger
    """

    def __init__(self):
        """Opens a session with the Apex API and initializes a logger."""
        self.session = ApexSession()
        self.logger = logging.getLogger(__name__)

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
            self.logger.info(f'Found {len(to_withdraw)} students in Apex'
                             ' not enrolled in PowerSchool.')
            student: ApexStudent
            for i, student in enumerate(to_withdraw):
                progress = f':{i + 1}/{len(to_withdraw)}:'
                self.logger.info(f'{progress}Removing student {student} '
                                 'from Apex.')
                r = student.delete_from_apex(session=self.session)
                self.logger.debug(f'Received status from delete request: '
                                  + str(r.status_code))
        else:
            self.logger.info('Apex roster agrees with PowerSchool')

    def sync_classroom_enrollment(self):
        self.init_enrollment()
        self.logger.info('Syncing classroom enrollments.')
        print(self.apex_enroll.classrooms & self.ps_enroll.classrooms)

    def enroll_students(self, student_ids: Collection[int]):
        apex_students = init_students_for_ids(student_ids)
        if len(apex_students) > 0:
            post_students(apex_students, session=self.session)

    def _has_enrollment(self):
        return hasattr(self, 'ps_enroll') and hasattr(self, 'apex_enroll')

    @property
    def apex_roster(self) -> Union[Set[int], KeysView]:
        """
        Avoids creating an ApexEnrollment object if it doesn't
        have to.
        """
        try:
            return self.apex_enroll.roster
        except AttributeError:
            try:
                return self._apex_roster
            except AttributeError:
                token = self.session.access_token
                self._apex_roster = set(ApexStudent.get_all(token=token,
                                                            ids_only=True))
                return self._apex_roster

    @property
    def ps_roster(self) -> KeysView:
        """Exists only to mirror the `apex_roster` method."""
        try:
            return self.ps_enroll.roster
        except AttributeError:
            self.ps_enroll = PSEnrollment()
            return self.ps_enroll.roster

    def sync_classrooms(self):
        """
        Ensures that all relevant classrooms that are present in
        PowerSchool appear in the Apex Learning database.
        """
        total = 0
        to_post = []
        for i, (section, progress) in enumerate(walk_ps_sections(archived=False)):
            try:
                """
                This will get checked again below, but if we can
                rule a section out before making a GET call to the
                Apex server, it saves an appreciable amount of time.
                """
                if not section['apex_program_code']:
                    self.logger.info(f'{progress}:Section {section["section_id"]} '
                                     'has no program codes. Skipping...')
                    continue
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            try:
                section_id = section['section_id']
                self.logger.info(f'{progress}:Attempting to fetch classroom with'
                                 f' ID {section_id}.')
                ApexClassroom.get(section_id, session=self.session)
                self.logger.info(f'{progress}:Classroom found.')
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            except exceptions.ApexObjectNotFoundException:
                self.logger.info(f'{progress}:Classroom not found in Apex. '
                                 'Creating classroom')
                try:
                    apex_obj = ApexClassroom.from_powerschool(section,
                                                              already_flat=True)
                    to_post.append(apex_obj)
                except exceptions.NoProductCodesException as e:
                    self.logger.info(e)
            except (exceptions.ApexNotAuthorizedError,
                    exceptions.ApexConnectionException):
                self.logger.exception('Failed to connect to Apex server.')
                return
            except exceptions.ApexError:
                self.logger.exception('Encountered unexpected error:\n')
            finally:
                total += 1

        r = ApexClassroom.post_batch(to_post, session=self.session)
        try:
            r.raise_for_status()
            self.logger.info(f'Added {len(to_post)}/{total} classrooms.')
        except requests.exceptions.HTTPError:
            print(r.text)


def init_students_for_ids(student_ids: Collection[int]) -> List[ApexStudent]:
    """
    Iterates over the PowerSchool rosters and creates ApexStudent
    objects out of the intersection between the IDs in `student_ids` and
    the PowerSchool students.

    :param student_ids: student EDUIDs
    :return: the students in both PowerSchool and the given last as
        `ApexStudent` objects
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Found {len(student_ids)} students in PowerSchool not'
                'enrolled in Apex.')
    ps_json = fetch_students()
    apex_students = []
    seen_eduids = set()
    for obj in ps_json:
        eduid = obj['tables']['students']['eduid']
        if not eduid:
            last_name = obj['tables']['students']['last_name']
            first_name = obj['tables']['students']['first_name']
            logger.info(f'Student "{first_name} {last_name}" does not have an'
                        'EDUID. Skipping...')
            continue

        eduid = int(eduid)
        if eduid in student_ids:
            if eduid in seen_eduids:
                logger.debug(f'Duplicate EDUID \"{eduid}\". Skipping...')
                continue
            try:
                logger.info(f'Creating student for EDUID {eduid}')
                apex_student = ApexStudent.from_powerschool(obj)
                seen_eduids.add(eduid)
                apex_students.append(apex_student)
            except ApexStudentNoEmailException:
                logger.info(f'Student with EDUID "{eduid}" has no email.'
                            'Skipping...')
            except ApexMalformedEmailException as e:
                logger.info(e)

    return apex_students


def post_students(apex_students: List[ApexStudent], token: TokenType = None,
                  session: requests.Session = None):
    """Posts a list of ApexStudents to the Apex API."""
    logger = logging.getLogger(__name__)
    logger.info(f'Posting {len(apex_students)} students.')
    r = ApexStudent.post_batch(apex_students, token=token, session=session)
    try:
        r.raise_for_status()
        logger.debug('Received status code ' + str(r.status_code))
    except requests.exceptions.HTTPError:
        logger.exception('Failed to POST students. Received status '
                         + str(r.status_code))

        as_json = r.json()
        if type(as_json) is dict:
            logger.info('Found duplicates.')
            put_duplicates(as_json, apex_students, token)
        elif type(as_json) is list:
            logger.info('Removing invalid entries.')
            repost_students(as_json, apex_students, token)
        else:
            logger.exception('Response text:\n' + r.text)


def put_duplicates(json_obj: dict, apex_students: List[ApexStudent],
                   token: TokenType = None, session: requests.Session = None):
    """
    A helper function for `post_students`. PUTs students that already
    exist in Apex.
    """
    logger = logging.getLogger(__name__)
    if not json_obj['HasError']:
        return

    duplicates = [user['Index'] for user in json_obj['studentUsers']]
    for student_idx in duplicates:
        student = apex_students[student_idx]
        logger.info(f'Putting student with EDUID {student.import_user_id}.')
        r = student.put_to_apex(token=token, session=session)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.info('PUT failed with response ' + str(e))


def repost_students(json_obj: dict, apex_students: List[ApexStudent],
                    token: TokenType = None, session: requests.Session = None):
    """
    Helper function for `post_students`. Removes invalid entries and
    attempts to POST again.
    """
    logger = logging.getLogger(__name__)
    to_retry = []
    for entry in json_obj:
        if entry['ValidationError']:
            logger.info(f'Student with EDUID {entry["ImportUserId"]} '
                        'did not pass validation.')
        else:
            to_retry.append(apex_students[entry['Index']])

    if to_retry:
        logger.info(f'Attempting to POST remaining {len(to_retry)} '
                    'students.')
        r = ApexStudent.post_batch(to_retry, token=token, session=session)
        try:
            r.raise_for_status()
            logger.info('Successfully POSTed remaining students.')
        except requests.exceptions.HTTPError:
            logger.exception(f'Failed to post {len(to_retry)} students.'
                             f'Received response:\n' + r.text)
    else:
        logger.info('No entries passed validation.')
