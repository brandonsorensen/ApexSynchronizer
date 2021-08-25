from __future__ import annotations
from typing import TYPE_CHECKING

from . import SyncDelegate
from apex_synchronizer import exceptions
from apex_synchronizer.apex_data_models import ApexStaffMember
from apex_synchronizer.ps_agent import fetch_staff

if TYPE_CHECKING:
    from ..apex_synchronizer import ApexSynchronizer


class StaffDelegate(SyncDelegate):

    """
    Adds all relevant PowerSchool staff to the Apex database. Does not
    remove staff.
    """

    def __init__(self, synchronizer: ApexSynchronizer):
        super().__init__(synchronizer)
        self.ps_staff = {}
        """All relevant staff in PowerSchool. Populated at runtime."""
        self.apex_staff = set()
        """All staff members in the Apex database. Populated at runtime."""

    def execute(self):
        self.init_staff()
        post_ids = self.ps_staff.keys() - self.apex_staff
        if len(post_ids) == 0:
            self.logger.info('Staff list in sync.')
            return

        if self.sync.dry_run:
            self.sync.operations['sync_staff'] = {
                'to_post': list(post_ids)
            }

        try:
            to_post = [self.ps_staff[id_] for id_ in post_ids]
        except KeyError:
            self.logger.exception('Internal logic error. Unrecognized key.')
            return
        try:
            self.logger.info(f'Posting {len(to_post)} staff member(s).')
            r = ApexStaffMember.post_batch(to_post, session=self.sync.session)
            errors = ApexStaffMember.parse_batch(r)
            self.logger.info('Received the following errors:\n'
                             + str({id_: error.name for id_, error
                                    in errors.items()}))
        except exceptions.ApexBatchTimeoutError as e:
            self.logger.info('POST operation lasted longer than '
                             f'{e.status_token} seconds. Will check again '
                             'before deconstructing.')
            self.sync.batch_jobs.append(e.status_token)

    def init_staff(self):
        """
        Initializes the `apex_staff` and `ps_staff` collections. This
        operation is lazy in that it occurs not when a `StaffDelegate`
        object is instantiated but rather when its sync operation is
        executed.
        """
        self.logger.debug('Fetching current ps_staff from Apex.')
        self.apex_staff = set(ApexStaffMember
                              .get_all_ids(session=self.sync.session))
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
