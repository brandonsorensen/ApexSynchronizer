import logging
from .apex_session import ApexSession
from .apex_data_models import ApexClassroom, ApexStudent, ApexDataObject
from .enrollment import ApexEnrollment, PSEnrollment


class ApexSynchronizer(object):

    def __init__(self):
        self.session = ApexSession()
        self.ps_enroll = PSEnrollment()
        self.logger = logging.getLogger(__name__)
        self.logger.info('Retrieved enrollment info from PowerSchool.')
        self.apex_enroll = ApexEnrollment()
        self.logger.info('Retrieved enrollment info from Apex.')

    def sync_rosters(self):
        self.logger.info('Beginning roster synchronization.')

        self.logger.info('Comparing enrollment information.')
        to_enroll = self.ps_enroll.roster - self.apex_enroll.roster
        if len(to_enroll) > 0:
            self.logger.info(f'Found {len(to_enroll)} students in PowerSchool not enrolled in Apex.')
            # TODO
        else:
            self.logger.info('Enrollment information is already in sync.')

        to_withdraw = self.apex_enroll.roster - self.ps_enroll.roster
        assert len(to_withdraw) > 0 or len(to_withdraw) == to_enroll

        # apex_classrooms = ApexClassroom.get_all(self.session.access_token)
        classrooms = [ApexClassroom.get(self.session.access_token, 80336)]
        self.logger.info('Retrieved Apex classrooms')

        students = ApexStudent.get_all(self.session.access_token)
        self.logger.info('Retrieved Apex students')

        student: ApexStudent
        for student in students:
            if int(student.import_org_id) == 616:
                print(self.ps_enroll.get_classrooms(student))
            else:
                print(student.import_org_id)

