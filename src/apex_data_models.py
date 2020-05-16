from collections import namedtuple



Student = namedtuple('Student', 'ImportUserId ImportOrgId FirstName \
                                 MiddleName LastName Email Role GradeLevel \
                                 LoginId LoginPw CoachEmails')

class Student(object):
    pass
