from __future__ import annotations
from collections import defaultdict
from typing import Tuple, TYPE_CHECKING

import requests

from apex_synchronizer import adm, exceptions
from apex_synchronizer.apex_data_models.apex_classroom import (ApexClassroom,
                                                               walk_ps_sections)

if TYPE_CHECKING:
    from . import ApexSynchronizer

from . import SyncDelegate


class ClassroomDelegate(SyncDelegate):

    def __init__(self, synchronizer: ApexSynchronizer):
        super().__init__(synchronizer)

        self.class_ops = defaultdict(list)
        self.total = self.updated = 0
        self.to_post = []
        self.progress = ''

    def add_to_post(self, section: dict):
        """
        Adds sections that do not exist in Apex to its database.
        :param section:
        :return:
        """
        self.logger.info(f'{self.progress}:Classroom not found in Apex. '
                         'Creating classroom.')
        try:
            apex_obj = ApexClassroom.from_powerschool(section,
                                                      already_flat=True)
            self.to_post.append(apex_obj)
        except exceptions.NoProductCodesException as e:
            self.logger.debug(e)

    @staticmethod
    def classrooms_equal(apex: ApexClassroom, powerschool: ApexClassroom) -> bool:
        """
        Determines whether two `ApexClassroom` objects. This method is
        necessary in addition to the the `ApexClassroom` class's magic
        method because classrooms in Apex's database can contain more
        than one product code, while PowerSchool only permits one. For
        the purposes of the sync, any `ApexClassroom` object whose
        product codes intersection with that of a PowerSchool
        counterpart will be considered equal, provided all other
        information matches.

        Due to limitations in Apex's API, it is not possible to remove
        product codes, so this extra step is necessary.

        :param apex: the `ApexClassroom` object from the Apex database
        :param powerschool: the `ApexClassroom` object from PowerSchool
        :return: whether the two classrooms are equal
        """
        if apex != powerschool:
            if set(powerschool.product_codes) <= set(apex.product_codes):
                # when Apex contains more than just the PS code
                powerschool.product_codes = apex.product_codes
                return apex == powerschool
            return False
        return True

    def execute(self):
        """
        Ensures that all relevant classrooms that are present in
        PowerSchool appear in the Apex Learning database.
        """
        for i, (section, progress) in enumerate(
                walk_ps_sections(archived=False, filter_date=False)
        ):
            self.progress = progress
            if not self.has_program_code(section):
                continue
            try:
                # Check equality, update if inconsistent
                apex_cr, ps_cr = self.get_classrooms(section)
                if not self.classrooms_equal(apex_cr, ps_cr):
                    self.logger.debug('Internal update for record '
                                      f'"{ps_cr.import_classroom_id}"')
                    self.class_ops['to_update'].append(
                        ps_cr.import_classroom_id
                    )
                    if not self.sync.dry_run:
                        self.update_classroom(apex_cr, ps_cr)
                    self.updated += 1
            except KeyError:
                raise exceptions.ApexMalformedJsonException(section)
            except exceptions.ApexObjectNotFoundException:
                # Object ID not found in Apex database
                self.add_to_post(section)
            except (exceptions.ApexNotAuthorizedError,
                    exceptions.ApexConnectionException):
                self.logger.exception('Failed to connect to Apex server.')
                return
            except exceptions.ApexError:
                self.logger.exception('Encountered unexpected error:\n')
            finally:
                self.total += 1

        if self.sync.dry_run:
            self.logger.info(f'Found {self.updated} classrooms to update.')
            self.logger.info(f'Found {len(self.to_post)} classrooms to add '
                             'to Apex.')
        else:
            self.logger.info(f'Updated {self.updated} classrooms.')
            self.logger.info(f'Posting {len(self.to_post)} classrooms.')
            self.post_classrooms()

        self.sync.operations['sync_classrooms'] = dict(self.class_ops)

    def get_classrooms(self, section: dict) \
            -> Tuple[ApexClassroom, ApexClassroom]:
        """
        Creates `ApexClassroom` objects for the Apex and PowerSchool
        sections.

        :param section: the relevant section as a `dict` object.
        :raises exception.ApexObjectNotFoundException: when the section
                cannot be located in the Apex database
        :return: a tuple containing two `ApexClassroom` objects.
        """
        section_id = section['section_id']
        if not section_id:
            self.logger.info('No classroom ID give for object below. '
                             'Skipping.\n' + str(section))
        self.logger.info(f'{self.progress}:Attempting to fetch classroom with'
                         f' ID {section_id}.')
        try:
            apex_cr = self.sync.apex_enroll.classroom_index[int(section_id)]
        except KeyError:
            raise exceptions.ApexObjectNotFoundException(section_id)
        ps_cr = ApexClassroom.from_powerschool(section,
                                               already_flat=True)
        self.logger.info(f'{self.progress}:Classroom found.')
        return apex_cr, ps_cr

    def has_program_code(self, section: dict) -> bool:
        """
        Checks whether a section has an Apex program code.
        :param section: the relevant section
        :return: whether the section has a program code
        """
        try:
            """
            This will get checked again below, but if we can
            rule a section out before making a GET call to the
            Apex server, it saves an appreciable amount of time.
            """
            if not section['apex_program_code']:
                self.logger.info(f'{self.progress}:Section {section["section_id"]} '
                                 'has no program codes. Skipping...')
                self.total += 1
                return False
            return True
        except KeyError:
            raise exceptions.ApexMalformedJsonException(section)

    def log_post_operations(self):
        """
        Add the collected POST operations to the parent
        `ApexSynchronizer` class's `operations` attribute.
        """
        if len(self.to_post) > 0:
            ops = []
            for cr in self.to_post:
                as_dict = cr.to_dict()
                as_dict['classroom_start_date'] = str(as_dict['classroom_start_date'])
                ops.append(as_dict)

            self.class_ops['to_post'] = ops

    def post_classrooms(self):
        """
        POST the new classrooms to the Apex database and parse the
        responses.
        """
        self.log_post_operations()
        r = ApexClassroom.post_batch(self.to_post, session=self.sync.session)
        # TODO: Parse response messages
        try:
            r.raise_for_status()
            self.logger.info(f'Added {len(self.to_post)}/{self.total} classrooms.')
        except requests.exceptions.HTTPError:
            errors = adm.apex_classroom.handle_400_response(r, self.logger)
            n_posted = len(self.to_post) - len(errors)
            self.logger.info(f'Received {len(errors)} errors:\n'
                             + str(errors))
            if n_posted:
                self.logger.info(f'Successfully added {n_posted} classrooms.')
            else:
                self.logger.info('No classrooms were added to Apex.')

    def update_classroom(self, apex_cr: ApexClassroom,
                         ps_cr: ApexClassroom):
        """
        Updates a classroom in the Apex database and the `ApexSynchronizer`
        class's enrollment objects. (See the `enrollment` module.)

        :param apex_cr: current Apex classroom
        :param ps_cr: PowerSchool equivalent used to update the Apex DB
        """
        r = ps_cr.put_to_apex(session=self.sync.session)
        try:
            r.raise_for_status()
            self.logger.debug('Updated classrooms with ID '
                              f'{ps_cr.import_classroom_id}.')
            apex_cr.update(ps_cr, session=self.sync.session)
            self.sync.apex_enroll.update_classroom(ps_cr)
        except requests.exceptions.HTTPError:
            self.logger.exception('Received bad response: '
                                  + str(r.status_code))
