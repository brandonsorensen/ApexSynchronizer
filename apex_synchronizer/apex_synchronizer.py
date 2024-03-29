from collections import defaultdict
from dataclasses import dataclass
from os import environ
from pathlib import Path
from typing import Collection, KeysView, List, Tuple, Set
import json
import logging
import os
import pickle
import time

import requests

from . import adm, exceptions
from .apex_data_models import ApexStudent, ApexClassroom, ApexStaffMember
from .apex_data_models.apex_classroom import walk_ps_sections
from .apex_schedule import ApexSchedule
from .apex_session import ApexSession
from .enrollment import ApexEnrollment, PSEnrollment
from .ps_agent import fetch_staff

PICKLE_DIR = Path('serial')


@dataclass
class StudentTuple(object):
    """
    A tuple containing a reference to an `ApexStudent` object and
    a PowerSchool student JSON-object.
    """
    apex: ApexStudent = None
    powerschool: ApexStudent = None

    def __iter__(self):
        return iter((self.apex, self.powerschool))

    def all(self):
        return self.apex and self.powerschool

    def matching(self):
        if not self.all():
            return False
        if self.powerschool.import_org_id == 615:
            self.powerschool.import_org_id += 1

        return self.apex.__eq__(self.powerschool, powerschool='r')

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

    def __init__(self, exclude=None):
        """Opens a session with the Apex API and initializes a logger."""
        self.session = ApexSession()
        self._dry_run = bool(int(environ.get('APEX_DRY_RUN', False)))
        if self._dry_run:
            self._operations = {}
        self.logger = logging.getLogger(__name__)
        self.batch_jobs = []
        self.ps_staff = {}
        self.apex_staff = set()
        self.apex_enroll, self.ps_enroll = self._init_enrollment(exclude)

    def save(self):
        """
        Has no effect when the `APEX_DRY_RUN` environment variable is not
        set to 1. Writes the operations that would have been executed in
        a "live" run to a JSON file.

        Note: This function was originally the magic function `__del__`,
        but in newer versions of Python, it ran into a bug in which
        the global `open` function was no longer in the namespace
        during object deconstruction.
        """
        try:
            if not self._dry_run:
                return
        except AttributeError:
            return

        with open('dry_run_info.json', 'w+') as f:
            json.dump(self._operations, f)

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

    def sync_rosters(self):
        self.logger.info('Beginning roster synchronization.')
        self.logger.info('Comparing enrollment information.')
        to_enroll = self.ps_roster - self.apex_roster
        to_withdraw = self.apex_roster - self.ps_roster
        to_update = self._find_conflicts()
        if len(to_enroll) == len(to_withdraw) == len(to_update) == 0:
            self.logger.info('Rosters already in sync')
            return

        if self._dry_run:
            ops = {}
            if len(to_enroll) > 0:
                ops['to_enroll'] = list(to_enroll)
            if len(to_withdraw) > 0:
                ops['to_withdraw'] = list(to_withdraw)
            if len(ops) > 0:
                self._operations['sync_roster'] = ops
            return

        if len(to_enroll) > 0:
            self._enroll_students(set(to_enroll))
        else:
            self.logger.info('PowerSchool roster agrees with Apex roster.')

        if len(to_update) > 0:
            for student in to_update:
                r = student.put_to_apex(session=self.session)
                try:
                    r.raise_for_status()
                    self.logger.info(f'Updated record for {student.import_user_id}.')
                except requests.HTTPError:
                    self.logger.exception('Received error response from PUT call:\n'
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
        self._init_staff()
        post_ids = self.ps_staff.keys() - self.apex_staff
        if len(post_ids) == 0:
            self.logger.info('Staff list in sync.')
            return

        if self._dry_run:
            self._operations['sync_staff'] = {
                'to_post': list(post_ids)
            }

        try:
            to_post = [self.ps_staff[id_] for id_ in post_ids]
        except KeyError:
            self.logger.exception('Internal logic error. Unrecognized key.')
            return
        try:
            self.logger.info(f'Posting {len(to_post)} staff member(s).')
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
            except KeyError:
                self.logger.info(f'Classroom bearing ID \"{c_id}\" is not in '
                                 'Apex. It must be added before syncing '
                                 'enrollment. Skipping for now...')
                continue

            apex_roster = set(self.apex_enroll.get_roster(c_id))

            student_list = set(student_list)
            if student_list == apex_roster:
                self.logger.info('Classroom enrollment in sync.')
                continue

            to_enroll = student_list - apex_roster
            to_withdraw = apex_roster - student_list

            ineligible = self._find_ineligible_enrollments(to_enroll)
            if len(ineligible) > 0:
                to_enroll -= ineligible
                self.logger.debug('The following students will not be enrolled:'
                                  f' {ineligible}')
            if len(to_enroll) == 0:
                self.logger.info('No eligible student to enroll.')
            else:
                self.logger.info(f'Adding {len(to_enroll)} students to classroom '
                                 + str(c_id))

                apex_to_enroll = [self.apex_enroll.get_student_for_id(id_)
                                  for id_ in to_enroll]
                if self._dry_run:
                    # TODO: Make this a defaultdict
                    try:
                        op = self._operations['sync_classroom_enrollment']
                        if 'to_enroll' in op.keys():
                            op['to_enroll'][c_id] = list(to_enroll)
                        else:
                            op['to_enroll'] = {c_id: list(to_enroll)}
                    except KeyError:
                        self._operations['sync_classroom_enrollment'] = {
                            'to_enroll': {c_id: list(to_enroll)}
                        }
                    n_updates = 0
                else:
                    n_updates = self._add_enrollments(to_enroll=apex_to_enroll,
                                                      classroom=apex_classroom)
                n_entries_changed += n_updates

            if to_withdraw and self._dry_run:
                try:
                    op = self._operations['sync_classroom_enrollment']
                    if 'to_withdraw' in op.keys():
                        op['to_withdraw'][c_id] = list(to_withdraw)
                    else:
                        op['to_withdraw'] = {c_id: list(to_withdraw)}
                except KeyError:
                    self._operations['sync_classroom_enrollment'] = {
                        'to_withdraw': {c_id: list(to_withdraw)}
                    }
                n_withdrawn = 0
            else:
                n_withdrawn = self._withdraw_enrollments(classroom=apex_classroom,
                                                         to_withdraw=to_withdraw)
            n_entries_changed += n_withdrawn
        self.logger.info(f'Updated {n_entries_changed} enrollment records.')

    def sync_classrooms(self):
        """
        Ensures that all relevant classrooms that are present in
        PowerSchool appear in the Apex Learning database.
        """
        total = 0
        updated = 0
        to_post = []
        class_ops = defaultdict(list)
        for i, (section, progress) in enumerate(walk_ps_sections(archived=False,
                                                                 filter_date=False)):
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
                    if self._dry_run:
                        class_ops['to_update'].append(ps_cr.import_classroom_id)
                    else:
                        r = ps_cr.put_to_apex(session=self.session)
                        try:
                            r.raise_for_status()
                            self.logger.debug('Updated classrooms with ID '
                                              f'{ps_cr.import_classroom_id}.')
                            apex_cr.update(ps_cr, session=self.session)
                            self.apex_enroll.update_classroom(ps_cr)
                        except requests.exceptions.HTTPError:
                            self.logger.exception('Received bad response: '
                                                  + str(r.status_code))
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

        if self._dry_run and len(to_post) > 0:
            ops = []
            for cr in to_post:
                as_dict = cr.to_dict()
                as_dict['classroom_start_date'] = str(as_dict['classroom_start_date'])
                ops.append(as_dict)

            class_ops['to_post'] = ops

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

        if self._dry_run:
            self._operations['sync_classrooms'] = dict(class_ops)

    def run_schedule(self, s: ApexSchedule):
        """
        Run all the routines specified in the :class:`ApexSchedule`
        object.

        :param s: an ApexSchedule object
        """
        as_dict = s.to_dict()
        pretty_string = json.dumps(as_dict, indent=2)
        self.logger.info('Received the following ApexSchedule\n'
                         + pretty_string)
        output = {}
        output['time'] = time.strftime('%Y-%m-%d %H:%M:%S %Z')
        output['schedule'] = as_dict

        method_status = {}
        for method_name, execute in s.to_dict().items():
            if execute:
                method_status[method_name] = 'started'
                method = getattr(self, method_name)
                self.logger.info(f'Executing routine: "{method_name}"')
                try:
                    method()
                    method_status[method_name] = 'success'
                except exceptions.ApexError:
                    method_status[method_name] = 'failed'

        output['status'] = method_status
        json.dump(output, open('last_sync_info.json', 'w+'))

    def _enroll_students(self, student_ids: Collection[str]):
        apex_students = self._init_students_for_ids(student_ids)
        if len(apex_students) > 0:
            self._post_students(apex_students)

    def _find_ineligible_enrollments(self, enrollments: Collection[int]) \
            -> Set[int]:
        ineligible = set()
        for ps_st in enrollments:
            if ps_st not in self.apex_enroll.roster:
                # TODO: Maybe the student should be added here?
                self.logger.debug(f'Student bearing ID "{ps_st}" is not in'
                                  ' Apex. He or she must be added before '
                                  'syncing enrollment. Skipping for now...')
                ineligible.add(ps_st)
                continue

        return ineligible

    def _find_conflicts(self) -> List[ApexStudent]:
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
        change_log = {}
        for st in transfer_map.values():
            if st.all() and not st.matching():
                self.logger.debug(f'Student {st.apex.import_user_id} will be'
                                  'updated to match PowerSchool records.')
                apex_json = st.apex.to_json()
                ps_json = st.powerschool.to_json()
                apex_only = apex_json.keys() - ps_json.keys()
                ps_only = ps_json.keys() - apex_json.keys()
                conflict = {k: (apex_json[k], ps_json[k])
                            for k in ps_json.keys() & apex_json.keys()
                            if k != 'LoginPw'
                            and apex_json[k] != ps_json[k]}
                for k in ps_only:
                    conflict[k] = (None, ps_json[k])
                for k in apex_only:
                    conflict[k] = (apex_json[k], None)
                if 'LoginPw' in conflict.keys():
                    del conflict['LoginPw']
                for k, (apex_value, ps_value) in conflict.items():
                    conflict[k] = {'apex': apex_value,
                                   'powerschool': ps_value}
                change_log[st.apex.import_user_id] = conflict
                st.update_apex()
                to_update.append(st.powerschool)

        if self._dry_run and change_log:
            if 'sync_roster' not in self._operations.keys():
                self._operations['sync_roster'] = {}
            self._operations['sync_roster']['to_update'] = change_log

        return to_update

    def _init_enrollment(self, exclude) -> \
            Tuple[ApexEnrollment, PSEnrollment]:
        use_serial = bool(int(environ.get('USE_PICKLE', False)))
        cache_apex = bool(int(environ.get('CACHE_APEX', False)))
        if not os.path.exists(PICKLE_DIR):
            os.makedirs(PICKLE_DIR, exist_ok=True)
        apex_path = PICKLE_DIR / 'apex_enroll.pickle'

        if use_serial:
            apex_enroll = pickle.load(open(apex_path, 'rb'))
        else:
            apex_enroll = ApexEnrollment(session=self.session,
                                         exclude=exclude)
            self.logger.info('Retrieved enrollment info from Apex.')
        ps_enroll = PSEnrollment(exclude=exclude)
        self.logger.info('Retrieved enrollment info from PowerSchool.')

        if cache_apex:
            self.logger.debug('Caching Apex roster to ' + str(apex_path))
            pickle.dump(apex_enroll, open(apex_path, 'wb+'))

        return apex_enroll, ps_enroll

    def _init_staff(self):
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

    def _init_students_for_ids(self, student_ids: Collection[str]) \
            -> List[ApexStudent]:
        """
        Iterates over the PowerSchool rosters and creates ApexStudent
        objects out of the intersection between the IDs in `student_ids` and
        the PowerSchool students.

        :param student_ids: student EDUIDs
        :return: the students in both PowerSchool and the given last as
            `ApexStudent` objects
        """
        self.logger.info(f'Found {len(student_ids)} students in PowerSchool '
                         'not enrolled in Apex.')
        apex_students: List[ApexStudent] = []
        for id_ in student_ids:
            try:
                student = self.ps_enroll.apex_index[id_]
                apex_students.append(student)
            except KeyError:
                self.logger.debug('Could not create ApexStudent object for '
                                  f'ID "{id_}". Cannot be added.')

        return apex_students

    def _has_enrollment(self):
        try:
            self.apex_enroll is not None and self.ps_enroll is not None
        except AttributeError:
            return False

    def _add_enrollments(self, to_enroll: Collection[ApexStudent],
                         classroom: ApexClassroom) -> int:
        id2student = {s.import_user_id: s for s in to_enroll}
        r = classroom.enroll(list(to_enroll), session=self.session)
        n_errors = 0
        already_exist = 0
        try:
            r.raise_for_status()
            for student in to_enroll:
                self.apex_enroll.add_to_classroom(s=student, c=classroom,
                                                  must_exist=False)
        except requests.HTTPError:
            to_json = r.json()
            msg = 'enrollment already exists'
            if 'studentUsers' in to_json.keys():
                for user in to_json['studentUsers']:
                    s_id = int(user['Code'])
                    if s_id == 200:
                        student = id2student[s_id]
                        self.apex_enroll.add_to_classroom(s=student,
                                                          c=classroom,
                                                          must_exist=False)
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
        n_updates = len(to_enroll) - n_errors

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

        return n_updates

    def _put_duplicates(self, json_obj: dict, apex_students: List[ApexStudent]):
        """
        A helper function for `post_students`. PUTs students that already
        exist in Apex.
        """
        if not json_obj['HasError']:
            return

        duplicates = [user['Index'] for user in json_obj['studentUsers']
                      if 'user already exist' in user['Message'].lower()]
        n_success = 0
        for student_idx in duplicates:
            student = apex_students[student_idx]
            self.logger.info('Putting student with ID '
                             f'{student.import_user_id}.')
            r = student.put_to_apex(session=self.session)
            try:
                r.raise_for_status()
                self.logger.debug('PUT operation successful.')
                self.apex_enroll.update_student(student)
                n_success += 1
            except requests.exceptions.HTTPError as e:
                self.logger.debug('PUT failed with response ' + str(e))

        if len(duplicates) > 0:
            self.logger.info(f'Successfully PUT {n_success}/{len(duplicates)} '
                             'students.')

    def _post_students(self, apex_students: List[ApexStudent]):
        """Posts a list of ApexStudents to the Apex API."""
        self.logger.info(f'Posting {len(apex_students)} students.')
        r = ApexStudent.post_batch(apex_students, session=self.session)
        try:
            r.raise_for_status()
            self.logger.debug('Received status code ' + str(r.status_code))
            for s in apex_students:
                self.apex_enroll.add_student(s)
        except requests.exceptions.HTTPError:
            as_json = r.json()
            if type(as_json) is dict:
                self.logger.info('Found duplicates.')
                self._put_duplicates(as_json, apex_students)
            elif type(as_json) is list:
                self.logger.info('Removing invalid entries.')
                self._repost_students(as_json, apex_students)
            else:
                self.logger.exception('Response text:\n' + r.text)

    def _repost_students(self, json_obj: dict,
                         apex_students: List[ApexStudent]):
        """
        Helper function for `post_students`. Removes invalid entries and
        attempts to POST again.
        """
        to_retry = []
        for entry in json_obj:
            if entry['ValidationError']:
                self.logger.info(f'Student with ID {entry["ImportUserId"]} '
                                 'did not pass validation.')
            else:
                to_retry.append(apex_students[entry['Index']])

        if to_retry:
            self.logger.info(f'Attempting to POST remaining {len(to_retry)} '
                             'students.')
            r = ApexStudent.post_batch(to_retry, session=self.session)
            try:
                r.raise_for_status()
                self.logger.info('Successfully POSTed remaining students.')
                for s in to_retry:
                    self.apex_enroll.add_student(s)
            except requests.exceptions.HTTPError:
                self.logger.exception(f'Failed to post {len(to_retry)} '
                                      'students. Received response:\n' + r.text)
        else:
            self.logger.info('No entries passed validation.')

    def _withdraw_enrollments(self, classroom: ApexClassroom,
                              to_withdraw: Collection[str]) -> int:
        """
        Withdraws students, whose IDs are given by `to_withdraw`
        from a a specified classroom. Returns a count of successful
        withdrawals.

        :param classroom: the classroom from which student will be
            withdrawn
        :param to_withdraw: the student to withdraw
        :return: the number of withdrawals that were successful
        """
        c_id = classroom.import_classroom_id
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
            r = classroom.withdraw(as_apex, session=self.session)
            try:
                r.raise_for_status()
                self.logger.debug('Successfully withdrawn.')
                self.apex_enroll.withdraw_student(s=as_apex, c=classroom)
                n_withdrawn += 1
            except requests.exceptions.HTTPError:
                self.logger.debug('Could not withdraw. Received response\n'
                                  + str(r.text))
        if len(to_withdraw) > 0:
            self.logger.info(f'Successfully withdrew {n_withdrawn}/'
                             f'{len(to_withdraw)} students from {c_id}.')
        return n_withdrawn
