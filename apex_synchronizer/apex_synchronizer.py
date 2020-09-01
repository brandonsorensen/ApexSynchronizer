from collections import defaultdict, KeysView
from dataclasses import dataclass
from operator import itemgetter
from os import environ
from pathlib import Path
from typing import Collection, List, Tuple
import json
import logging
import pickle

import requests

from . import adm, exceptions
from .apex_data_models import ApexStudent, ApexClassroom, ApexStaffMember
from .apex_data_models.apex_classroom import walk_ps_sections
from .apex_schedule import ApexSchedule
from .apex_session import ApexSession, TokenType
from .enrollment import ApexEnrollment, PSEnrollment, PSStudent
from .exceptions import ApexNoEmailException, ApexMalformedEmailException
from .ps_agent import fetch_students, fetch_staff

PICKLE_DIR = Path('serial')


@dataclass
class StudentTuple(object):
    """
    A tuple containing a reference to an `ApexStudent` object and
    a PowerSchool student JSON-object.
    """
    apex: ApexStudent = None
    powerschool: PSStudent = None

    def __iter__(self):
        return iter((self.apex, self.powerschool))

    def all(self):
        return self.apex and self.powerschool

    def matching(self):
        if not self.all():
            return False
        if self.powerschool.import_org_id == 615:
            self.powerschool.import_org_id += 1
        return (
            int(self.apex.import_user_id) == self.powerschool.import_user_id
            and int(self.apex.import_org_id) == self.powerschool.import_org_id
        )

    def update_apex(self):
        if self.all():
            self.apex.import_org_id = self.powerschool.import_org_id


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
        self.batch_jobs = []
        self.ps_staff = {}
        self.apex_enroll, self.ps_enroll = self.init_enrollment()

    def run_schedule(self, s: ApexSchedule):
        """
        Run all the routines specified in the :class:`ApexSchedule`
        object.

        :param s: an ApexSchedule object
        """
        pretty_string = json.dumps(s.to_dict(), indent=2)
        self.logger.info('Received the following ApexSchedule\n'
                         + pretty_string)
        for method_name, execute in s.to_dict().items():
            if execute:
                method = getattr(self, method_name)
                self.logger.info(f'Executing routine: "{method_name}"')
                method()

    def init_enrollment(self) -> Tuple[ApexEnrollment, PSEnrollment]:
        use_serial = bool(int(environ.get('USE_PICKLE', False)))
        cache_apex = bool(int(environ.get('CACHE_APEX', False)))
        apex_path = PICKLE_DIR / 'apex_enroll.pickle'
        if use_serial:
            apex_enroll = pickle.load(open(apex_path, 'rb'))
        else:
            apex_enroll = ApexEnrollment(session=self.session)
            self.logger.info('Retrieved enrollment info from Apex.')
        ps_enroll = PSEnrollment()
        self.logger.info('Retrieved enrollment info from PowerSchool.')

        if cache_apex:
            self.logger.debug('Caching Apex roster to ' + str(apex_path))
            pickle.dump(apex_enroll, open(apex_path, 'wb+'))

        return apex_enroll, ps_enroll

    def init_staff(self):
        self.logger.debug('Fetching current ps_staff from Apex.')
        self.apex_staff = set(ApexStaffMember.get_all_ids(session=self.session))
        self.ps_staff = {}
        self.logger.info('Fetching staff from PowerSchool.')
        for sm in fetch_staff():
            try:
                apex_sm = ApexStaffMember.from_powerschool(sm)
                if int(apex_sm.import_org_id) in (501, 616):
                    self.ps_staff[apex_sm.import_user_id] = apex_sm
            except exceptions.ApexEmailException as e:
                self.logger.debug(e)
        self.logger.info(f'Successfully retrieved {len(self.ps_staff)} '
                         'ps_staff members from PowerSchool.')

    def sync_rosters(self):
        self.logger.info('Beginning roster synchronization.')
        self.logger.info('Comparing enrollment information.')
        to_enroll = self.ps_roster - self.apex_roster
        to_withdraw = self.apex_roster - self.ps_roster
        to_update = self.find_conflicts()
        if len(to_enroll) == len(to_withdraw) == len(to_update) == 0:
            self.logger.info('Rosters already in sync')
            return

        if len(to_enroll) > 0:
            self.enroll_students(set(to_enroll))
        else:
            self.logger.info('PowerSchool roster agrees with Apex roster.')

        if len(to_update) > 0:
            for student in to_update:
                r = student.put_to_apex(session=self.session)
                self.logger.info('Recieved response from PUT call:\n'
                                 + str(r.text))
        else:
            self.logger.info('All records match one another. None to update.')

        if len(to_withdraw) > 0:
            self.logger.info(f'Found {len(to_withdraw)} students in Apex'
                             ' not enrolled in PowerSchool.')
            ApexStudent.delete_batch(to_withdraw, session=self.session)
        else:
            self.logger.info('Apex roster agrees with PowerSchool')

    def sync_staff(self):
        self.init_staff()
        self.logger.info('Posting staff members.')
        post_ids = self.ps_staff.keys() - self.apex_staff
        try:
            to_post = itemgetter(*post_ids)(self.ps_staff)
        except KeyError:
            self.logger.exception('Internal logic error. Unrecognized key.')
            return
        try:
            r = ApexStaffMember.post_batch(to_post, session=self.session)
            errors = ApexStaffMember.parse_batch(r)
            self.logger.info('Received the following errors:\n'
                             + str({id_: error.name for id_, error
                                    in errors.items()}))
        except exceptions.ApexBatchTimeoutError as e:
            self.logger.info('POST operation lasted longer than '
                             f'{e.status_token} seconds. Will check again '
                             'before deconstructing.')
            self.batch_jobs.append(e.status_token)

    def sync_classroom_enrollment(self):
        self.logger.info('Syncing classroom enrollments.')
        c2s = self.ps_enroll.classroom2students.items()
        n_classrooms = len(c2s)
        n_entries_changed = 0
        for i, (c_id, student_list) in enumerate(c2s):
            self.logger.info(f'{i + 1}/{n_classrooms}: Checking enrollment '
                             f'for classroom with ID \"{c_id}\".')
            try:
                apex_classroom = self.apex_enroll.get_classroom_for_id(c_id)
                apex_roster = set(self.apex_enroll.get_roster(c_id))
            except KeyError:
                self.logger.info(f'Classroom bearing ID \"{c_id}\" is not in '
                                 'Apex. It must be added before syncing '
                                 'enrollment. Skipping for now...')
                continue

            student_list = set(student_list)
            if student_list == apex_roster:
                self.logger.info('Classroom enrollment in sync.')
                continue

            to_enroll = student_list - apex_roster
            to_withdraw = apex_roster - student_list
            ineligible = set()
            for ps_st in to_enroll:
                if ps_st not in self.apex_enroll.roster:
                    # TODO: Maybe the student should be added here?
                    self.logger.debug(f'Student bearing ID "{ps_st}" is not in'
                                      ' Apex. He or she must be added before '
                                      'syncing enrollment. Skipping for now...')
                    ineligible.add(ps_st)
                    continue
            if len(ineligible) > 0:
                to_enroll -= ineligible
                self.logger.debug('The following students will not be enrolled:'
                                  f' {ineligible}')
            if len(to_enroll) == 0:
                self.logger.info('No eligible student to enroll.')
                continue

            self.logger.info(f'Adding {len(to_enroll)} students to classroom '
                             + str(c_id))

            apex_to_enroll = [self.apex_enroll.get_student_for_id(eduid)
                              for eduid in to_enroll]
            r = apex_classroom.enroll(apex_to_enroll, session=self.session)
            n_errors = 0
            already_exist = 0
            try:
                r.raise_for_status()
            except requests.HTTPError:
                to_json = r.json()
                msg = 'enrollment already exists'
                if 'studentUsers' in to_json.keys():
                    for user in to_json['studentUsers']:
                        if int(user['Code']) == 200:
                            continue
                        if user['Message'].lower().startswith(msg):
                            user_id = user['ImportUserId']
                            self.logger.debug(f'Student \"{user_id}\" already '
                                              'enrolled.')
                            already_exist += 1

                        else:
                            self.logger.info('Could not add student. Received '
                                             'the following error:\n'
                                             + str(user))
                        n_errors += 1
                else:
                    raise exceptions.ApexMalformedJsonException(to_json)
            n_entries_changed += len(to_enroll) - n_errors
            if n_errors:
                self.logger.debug(f'Received {n_errors} errors.')

            if already_exist:
                pct_already_exist = already_exist / n_errors
                no_changes = pct_already_exist == 1
                if no_changes:
                    self.logger.info('No entries were updated.')
                else:
                    self.logger.debug(f'Of {n_errors} errors, {already_exist} '
                                      f'({pct_already_exist * 100}%) are \"already'
                                      ' exists\" errors.')

            if len(to_withdraw) > 0:
                self.logger.info(f'Withdrawing {len(to_withdraw)} students from'
                                 f' classroom "{c_id}".')
            n_withdrawn = 0
            for s in to_withdraw:
                self.logger.debug(f'Withdrawing {s}.')
                try:
                    as_apex = self.apex_enroll.get_student_for_id(s)
                except KeyError:
                    # This should be impossible, but I'll still catch it
                    self.logger.debug('Cannot withdraw student who is not '
                                      'already enrolled in Apex: ' + str(s))
                    continue
                r = apex_classroom.withdraw(as_apex, session=self.session)
                try:
                    r.raise_for_status()
                    self.logger.debug('Successfully withdrawn.')
                    n_withdrawn += 1
                except requests.exceptions.HTTPError:
                    self.logger.debug('Could not withdraw. Received response\n'
                                      + str(r.text))
            if len(to_withdraw) > 0:
                self.logger.info(f'Successfully withdrew {n_withdrawn}/'
                                 f'{len(to_withdraw)} students from {c_id}.')
                n_entries_changed += n_withdrawn
        self.logger.info(f'Updated {n_entries_changed} enrollment records.')

    def enroll_students(self, student_ids: Collection[int]):
        apex_students = init_students_for_ids(student_ids)
        if len(apex_students) > 0:
            post_students(apex_students, session=self.session)

    def _has_enrollment(self):
        try:
            self.apex_enroll is not None and self.ps_enroll is not None
        except AttributeError:
            return False

    @property
    def apex_roster(self) -> KeysView:
        """
        Avoids creating an ApexEnrollment object if it doesn't
        have to.
        """
        return self.apex_enroll.roster

    @property
    def ps_roster(self) -> KeysView:
        """Exists only to mirror the `apex_roster` method."""
        return self.ps_enroll.roster

    def sync_classrooms(self):
        """
        Ensures that all relevant classrooms that are present in
        PowerSchool appear in the Apex Learning database.
        """
        total = 0
        updated = 0
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
                    total += 1
                    continue
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            try:
                section_id = section['section_id']
                if not section_id:
                    self.logger.info('No classroom ID give for object below. '
                                     'Skipping.\n' + str(section))
                self.logger.info(f'{progress}:Attempting to fetch classroom with'
                                 f' ID {section_id}.')
                apex_cr = ApexClassroom.get(section_id, session=self.session)
                ps_cr = ApexClassroom.from_powerschool(section,
                                                       already_flat=True)
                self.logger.info(f'{progress}:Classroom found.')
                if apex_cr != ps_cr:
                    if set(ps_cr.product_codes) <= set(apex_cr.product_codes):
                        # when Apex contains more than just the PS code
                        ps_cr.product_codes = apex_cr.product_codes
                        if apex_cr == ps_cr:
                            continue
                    self.logger.info('Updating record '
                                     + str(ps_cr.import_classroom_id))
                    r = ps_cr.put_to_apex(session=self.session)
                    self.logger.info('Received response: ' + str(r.status_code))
                    apex_cr.update(ps_cr, session=self.session)
                    self.apex_enroll.update_classroom(ps_cr)
                    updated += 1
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

        self.logger.info(f'Updated {updated} classrooms.')
        self.logger.info(f'Posting {len(to_post)} classrooms.')
        r = ApexClassroom.post_batch(to_post, session=self.session)
        # TODO: Parse response messages
        try:
            r.raise_for_status()
            self.logger.info(f'Added {len(to_post)}/{total} classrooms.')
        except requests.exceptions.HTTPError:
            errors = adm.apex_classroom.handle_400_response(r, self.logger)
            n_posted = len(to_post) - len(errors)
            self.logger.info(f'Received {len(errors)} errors:\n'
                             + str(errors))
            if n_posted:
                self.logger.info(f'Successfully added {n_posted} classrooms.')
            else:
                self.logger.info('No classrooms were added to Apex.')

    def find_conflicts(self) -> List[ApexStudent]:
        """
        Finds all students who need to be updated.
        :return: a list of student with updated information that can be
            used in PUT calls to the Apex server
        """
        transfer_map = defaultdict(StudentTuple)

        self.logger.debug('Adding Apex students to transfer map.')
        for student in self.apex_enroll.students:
            transfer_map[student.import_user_id].apex = student

        self.logger.debug('Adding PowerSchool student to transfer map.')
        for student in self.ps_enroll.students:
            transfer_map[student.import_user_id].powerschool = student

        self.logger.debug('Searching for mismatching records.')
        to_update = []
        for st in transfer_map.values():
            if st.all() and not st.matching():
                self.logger.debug(f'Student {st.apex.import_user_id} will be'
                                  'updated to match PowerSchool records.')
                st.update_apex()
                to_update.append(st.apex)

        return to_update


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
            logger.info(f'Student "{first_name} {last_name}" does not have an '
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
            except ApexNoEmailException:
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
        as_json = r.json()
        if type(as_json) is dict:
            logger.info('Found duplicates.')
            put_duplicates(as_json, apex_students, token=token,
                           session=session)
        elif type(as_json) is list:
            logger.info('Removing invalid entries.')
            repost_students(as_json, apex_students, token=token,
                            session=session)
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

    duplicates = [user['Index'] for user in json_obj['studentUsers']
                  if 'user already exist' in user['Message'].lower()]
    n_success = 0
    for student_idx in duplicates:
        student = apex_students[student_idx]
        logger.info(f'Putting student with EDUID {student.import_user_id}.')
        r = student.put_to_apex(token=token, session=session)
        try:
            r.raise_for_status()
            logger.debug('PUT operation successful.')
            n_success += 1
        except requests.exceptions.HTTPError as e:
            logger.debug('PUT failed with response ' + str(e))

    if len(duplicates) > 0:
        logger.info(f'Successfully PUT {n_success}/{len(duplicates)} '
                    f'students.')


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

