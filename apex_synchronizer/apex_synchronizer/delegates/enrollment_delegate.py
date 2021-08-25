from collections import Collection, Set

import requests

from . import SyncDelegate
from apex_synchronizer import exceptions
from apex_synchronizer.apex_data_models import ApexClassroom, ApexStudent


class EnrollmentDelegate(SyncDelegate):

    def execute(self):
        self.logger.info('Syncing classroom enrollments.')
        c2s = self.sync.ps_enroll.classroom2students.items()
        n_classrooms = len(c2s)
        n_entries_changed = 0
        for i, (c_id, student_list) in enumerate(c2s):
            self.logger.info(f'{i + 1}/{n_classrooms}: Checking enrollment '
                             f'for classroom with ID \"{c_id}\".')
            try:
                apex_classroom = self.sync.apex_enroll.get_classroom_for_id(c_id)
            except KeyError:
                self.logger.info(f'Classroom bearing ID \"{c_id}\" is not in '
                                 'Apex. It must be added before syncing '
                                 'enrollment. Skipping for now...')
                continue

            apex_roster = set(self.sync.apex_enroll.get_roster(c_id))

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

                apex_to_enroll = [self.sync.apex_enroll.get_student_for_id(id_)
                                  for id_ in to_enroll]
                if self.sync.dry_run:
                    # TODO: Make this a defaultdict
                    try:
                        op = self.sync.operations['sync_classroom_enrollment']
                        if 'to_enroll' in op.keys():
                            op['to_enroll'][c_id] = list(to_enroll)
                        else:
                            op['to_enroll'] = {c_id: list(to_enroll)}
                    except KeyError:
                        self.sync.operations['sync_classroom_enrollment'] = {
                            'to_enroll': {c_id: list(to_enroll)}
                        }
                    n_updates = 0
                else:
                    n_updates = self._add_enrollments(to_enroll=apex_to_enroll,
                                                      classroom=apex_classroom)
                n_entries_changed += n_updates

            if to_withdraw and self.sync.dry_run:
                try:
                    op = self.sync.operations['sync_classroom_enrollment']
                    if 'to_withdraw' in op.keys():
                        op['to_withdraw'][c_id] = list(to_withdraw)
                    else:
                        op['to_withdraw'] = {c_id: list(to_withdraw)}
                except KeyError:
                    self.sync.operations['sync_classroom_enrollment'] = {
                        'to_withdraw': {c_id: list(to_withdraw)}
                    }
                n_withdrawn = 0
            else:
                n_withdrawn = self._withdraw_enrollments(classroom=apex_classroom,
                                                         to_withdraw=to_withdraw)
            n_entries_changed += n_withdrawn
        self.logger.info(f'Updated {n_entries_changed} enrollment records.')

    def _add_enrollments(self, to_enroll: Collection[ApexStudent],
                         classroom: ApexClassroom) -> int:
        id2student = {s.import_user_id: s for s in to_enroll}
        r = classroom.enroll(list(to_enroll), session=self.sync.session)
        n_errors = 0
        already_exist = 0
        try:
            r.raise_for_status()
            for student in to_enroll:
                self.sync.apex_enroll.add_to_classroom(s=student, c=classroom,
                                                       must_exist=False)
        except requests.HTTPError:
            to_json = r.json()
            msg = 'enrollment already exists'
            if 'studentUsers' in to_json.keys():
                for user in to_json['studentUsers']:
                    s_id = int(user['Code'])
                    if s_id == 200:
                        student = id2student[s_id]
                        self.sync.apex_enroll.add_to_classroom(s=student,
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

    def _find_ineligible_enrollments(self, enrollments: Collection[int]) \
            -> Set[int]:
        ineligible = set()
        for ps_st in enrollments:
            if ps_st not in self.sync.apex_enroll.roster:
                # TODO: Maybe the student should be added here?
                self.logger.debug(f'Student bearing ID "{ps_st}" is not in'
                                  ' Apex. He or she must be added before '
                                  'syncing enrollment. Skipping for now...')
                ineligible.add(ps_st)
                continue

        return ineligible

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
                as_apex = self.sync.apex_enroll.get_student_for_id(s)
            except KeyError:
                # This should be impossible, but I'll still catch it
                self.logger.debug('Cannot withdraw student who is not '
                                  'already enrolled in Apex: ' + str(s))
                continue
            r = classroom.withdraw(as_apex, session=self.sync.session)
            try:
                r.raise_for_status()
                self.logger.debug('Successfully withdrawn.')
                self.sync.apex_enroll.withdraw_student(s=as_apex, c=classroom)
                n_withdrawn += 1
            except requests.exceptions.HTTPError:
                self.logger.debug('Could not withdraw. Received response\n'
                                  + str(r.text))
        if len(to_withdraw) > 0:
            self.logger.info(f'Successfully withdrew {n_withdrawn}/'
                             f'{len(to_withdraw)} students from {c_id}.')
        return n_withdrawn
