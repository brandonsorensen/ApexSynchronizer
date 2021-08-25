from collections import KeysView
from datetime import datetime
from os import environ
from typing import List, Tuple
import json
import logging
import os
import pickle
import time

from . import delegates
from .constants import PICKLE_DIR
from .. import exceptions
from ..apex_schedule import ApexSchedule
from ..apex_session import ApexSession
from ..enrollment import ApexEnrollment, PSEnrollment


class ApexSynchronizer(object):

    """
    A driver class that synchronizes data between the PowerSchool and
    Apex databases. In general, it treats the PowerSchool database as
    a "master" copy that Apex data should match.
    """

    def __init__(self, exclude=None):
        """Opens a session with the Apex API and initializes a logger."""
        self.session = ApexSession()
        self.logger = logging.getLogger(__name__)
        self.dry_run = bool(int(environ.get('APEX_DRY_RUN', False)))
        """True indicates operations should only be logged, not carried out."""
        self.operations = {}
        """A log of all the operations that were/should be executed."""
        self.batch_jobs = []
        self.apex_enroll, self.ps_enroll = self._init_enrollment(exclude)

        self.sync_roster = delegates.RosterDelegate(self)
        self.sync_classrooms = delegates.ClassroomDelegate(self)
        self.sync_classroom_enrollments = delegates.EnrollmentDelegate(self)
        self.sync_roster = delegates.StaffDelegate(self)

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
        output = {
            'time': time.strftime('%Y-%m-%d %H:%M:%S %Z'),
            'schedule': as_dict
        }

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
        with open('dry_run_info.json', 'w+') as f:
            self.operations['dry_run'] = self.dry_run
            self.operations['runtime'] = (datetime.now()
                                          .strftime('%Y-%m-%d %H:%M:%S %Z'))
            json.dump(self.operations, f)

    def _init_enrollment(self, exclude: List[str]) -> \
            Tuple[ApexEnrollment, PSEnrollment]:
        """
        Initializes `ApexEnrollment` and `PSEnrollment` objects.

        Utilizes two environment variables:

        `USE_PICKLE` of type `boolean` determines whether the
        `ApexEnrollment` object will be read off disk from a serialized
        object. Defaults to False.

        `CACHE_APEX` of type `boolean` indicates whether the initialized
        `ApexEnrollment` object should be serialized to disk.

        :param exclude: students to exclude from sync
        :return: a tuple containing initialized enrollment objects. Apex
            first, PowerSchool second.
        """
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

