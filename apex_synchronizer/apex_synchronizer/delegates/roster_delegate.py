from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Collection, List, TYPE_CHECKING

import requests

from . import SyncDelegate
from apex_synchronizer import adm
from apex_synchronizer.apex_data_models import ApexStudent

if TYPE_CHECKING:
    from apex_synchronizer import ApexSynchronizer


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
        """If tuple has both Apex and PowerSchool objects."""
        return self.apex and self.powerschool

    def matching(self):
        """Whether the two objects are equivalent."""
        if not self.all():
            return False
        if self.powerschool.import_org_id == 615:
            self.powerschool.import_org_id += 1

        return self.apex.__eq__(self.powerschool, powerschool='r')

    def get_conflicts(self) -> dict:
        """
        Finds the fields where the Apex and PowerSchool objects are in
        conflict and returns them as a `dict` object.
        """
        apex_json = self.apex.to_json()
        ps_json = self.powerschool.to_json()
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
        return conflict

    def update_apex(self):
        """
        Updates the Apex object with values from the PowerSchool object.
        """
        if self.all():
            self.apex.import_org_id = self.powerschool.import_org_id


class RosterDelegate(SyncDelegate):

    """
    Defines the sync behavior for student objects. This algorithm
    has operations for creating, updating, and deleting students from
    Apex. It is a true sync in that it attempts to directly mirror
    the PowerSchool database.

    There are three attributes that reference containers that hold the
    IDs of students that are to be enrolled, withdrawn, or updated.
    """

    def __init__(self, synchronizer: ApexSynchronizer):
        super().__init__(synchronizer)
        """Attributes are initialized empty and only populated if sync is executed."""
        self.to_enroll = self.to_withdraw = self.to_update = []
        self.ops = self.change_log = {}

    def enroll_students(self):
        if len(self.to_enroll) > 0:
            apex_students = self.init_students_for_ids(set(self.to_enroll))
            if len(apex_students) > 0:
                self.post_students(apex_students)
        else:
            self.logger.info('PowerSchool roster agrees with Apex roster.')

    def execute(self):
        self.logger.info('Beginning roster synchronization.')
        self.logger.info('Comparing enrollment information.')
        self.load_operations()
        if len(self.to_enroll) == len(self.to_withdraw) == len(self.to_update) == 0:
            self.logger.info('Rosters already in sync')
            return

        self.log_operations()
        if not self.sync.dry_run:
            self.enroll_students()
            self.update_students()
            self.withdraw_students()
        self.logger.info('Roster sync complete.')

    def find_conflicts(self):
        """
        Finds all students who need to be updated and populates the
        `to_update` attribute with those objects.
        """
        transfer_map = defaultdict(StudentTuple)

        self.logger.debug('Adding Apex students to transfer map.')
        for student in self.sync.apex_enroll.students:
            transfer_map[student.import_user_id].apex = student

        self.logger.debug('Adding PowerSchool student to transfer map.')
        for student in self.sync.ps_enroll.students:
            transfer_map[student.import_user_id].powerschool = student

        self.logger.debug('Searching for mismatching records.')
        for st in transfer_map.values():
            if st.all() and not st.matching():
                self.logger.debug(f'Student {st.apex.import_user_id} will be'
                                  'updated to match PowerSchool records.')
                self.change_log[st.apex.import_user_id] = st.get_conflicts()
                st.update_apex()
                self.to_update.append(st.powerschool)

    def init_students_for_ids(self, student_ids: Collection[str]) \
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
                student = self.sync.ps_enroll.apex_index[id_]
                apex_students.append(student)
            except KeyError:
                self.logger.debug('Could not create ApexStudent object for '
                                  f'ID "{id_}". Cannot be added.')

        return apex_students

    def load_operations(self):
        """
        Populates `to_enroll`, `to_withdraw`, and `to_update` with their
        respective operations.
        """
        self.to_enroll = self.sync.ps_roster - self.sync.apex_roster
        self.to_withdraw = self.sync.apex_roster - self.sync.ps_roster
        self.find_conflicts()

    def log_operations(self):
        ops = {}
        if len(self.to_enroll) > 0:
            ops['to_enroll'] = list(self.to_enroll)
        if len(self.to_withdraw) > 0:
            ops['to_withdraw'] = list(self.to_withdraw)
        if len(self.change_log) > 0:
            ops['to_update'] = self.change_log

        if len(ops) > 0:
            self.sync.operations['sync_roster'] = ops

    def post_students(self, apex_students: List[ApexStudent]):
        """Posts a list of ApexStudents to the Apex API."""
        self.logger.info(f'Posting {len(apex_students)} students.')
        r = ApexStudent.post_batch(apex_students, session=self.sync.session)
        try:
            r.raise_for_status()
            self.logger.debug('Received status code ' + str(r.status_code))
            for s in apex_students:
                self.sync.apex_enroll.add_student(s)
        except requests.exceptions.HTTPError:
            as_json = r.json()
            if type(as_json) is dict:
                self.logger.debug('Found duplicates.')
                self.put_duplicates(as_json, apex_students)
            elif type(as_json) is list:
                self.logger.debug('Removing invalid entries.')
                self.repost_students(as_json, apex_students)
            else:
                self.logger.exception('Response text:\n' + r.text)

    def put_duplicates(self, json_obj: dict, apex_students: List[ApexStudent]):
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
            self.logger.debug('Putting student with ID '
                              f'{student.import_user_id}.')
            r = student.put_to_apex(session=self.sync.session)
            try:
                r.raise_for_status()
                self.logger.debug('PUT operation successful.')
                self.sync.apex_enroll.update_student(student)
                n_success += 1
            except requests.exceptions.HTTPError as e:
                self.logger.debug('PUT failed with response ' + str(e))

        if len(duplicates) > 0:
            self.logger.debug(f'Successfully PUT {n_success}/{len(duplicates)} '
                              'students.')

    def repost_students(self, json_obj: dict,
                         apex_students: List[ApexStudent]):
        """
        Helper function for `post_students`. Removes invalid entries and
        attempts to POST again.
        """
        to_retry = []
        for entry in json_obj:
            if entry['ValidationError']:
                self.logger.debug(f'Student with ID {entry["ImportUserId"]} '
                                  'did not pass validation.')
            else:
                to_retry.append(apex_students[entry['Index']])

        if to_retry:
            self.logger.debug(f'Attempting to POST remaining {len(to_retry)} '
                              'students.')
            r = ApexStudent.post_batch(to_retry, session=self.sync.session)
            try:
                r.raise_for_status()
                self.logger.info('Successfully POSTed remaining students.')
                for s in to_retry:
                    self.sync.apex_enroll.add_student(s)
            except requests.exceptions.HTTPError:
                self.logger.exception(f'Failed to post {len(to_retry)} '
                                      'students. Received response:\n' + r.text)
        else:
            self.logger.info('No entries passed validation.')

    def update_students(self):
        """Updates students in the Apex database to match PowerSchool."""
        if len(self.to_update) > 0:
            for student in self.to_update:
                r = student.put_to_apex(session=self.sync.session)
                try:
                    r.raise_for_status()
                    self.logger.debug('Updated record for '
                                      f'{student.import_user_id}.')
                except requests.HTTPError:
                    self.logger.exception('Received error response from PUT call:\n'
                                          + str(r.text))
        else:
            self.logger.info('All records match one another. None to update.')

    def withdraw_students(self):
        """Withdraws students from Apex."""
        if len(self.to_withdraw) > 0:
            self.logger.info(f'Found {len(self.to_withdraw)} students in Apex'
                             ' not enrolled in PowerSchool.')
            adm.ApexStudent.delete_batch(self.to_withdraw,
                                         session=self.sync.session)
        else:
            self.logger.info('Apex roster agrees with PowerSchool')

