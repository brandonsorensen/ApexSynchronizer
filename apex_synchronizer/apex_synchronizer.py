import logging
from .apex_session import ApexSession
from .apex_data_models import ApexClassroom, ApexStudent, ApexDataObject
from .enrollment import ApexEnrollment, PSEnrollment


class ApexSynchronizer(object):

    def __init__(self):
        self.session = ApexSession()
        self.logger = logging.getLogger(__name__)

    def sync_rosters(self):
        self.logger.info('Beginning roster synchronization.')

        ps_enroll = PSEnrollment()
        self.logger.info('Retrieved enrollment info from PowerSchool.')
        apex_enroll = ApexEnrollment(access_token=self.session.access_token)
        self.logger.info('Retrieved enrollment info from Apex.')

        self.logger.info('Comparing enrollment information.')
        to_enroll = ps_enroll.roster - apex_enroll.roster
        if len(to_enroll) > 0:
            self.logger.info(f'Found {len(to_enroll)} students in PowerSchool not enrolled in Apex.')
            # TODO
        else:
            self.logger.info('Enrollment information is already in sync.')

        to_withdraw = apex_enroll.roster - ps_enroll.roster
        assert len(to_withdraw) > 0 or len(to_withdraw) == to_enroll

        # apex_classrooms = ApexClassroom.get_all(self.session.access_token)
        classrooms = [ApexClassroom.get(self.session.access_token, 80336)]
        self.logger.info('Retrieved Apex objects')

        students = ApexStudent.get_all(self.session.access_token)
        self.logger.info('Retrieved Apex students')

        student: ApexStudent
        for student in students:
            if int(student.import_org_id) == 616:
                print(ps_enroll.get_classrooms(student))
            else:
                print(student.import_org_id)

