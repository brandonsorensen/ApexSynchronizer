from typing import Union


class ApexError(Exception):

    def __str__(self):
        return 'There was an error when interfacing with the Apex API.'


class ApexConnectionException(ApexError):

    def __str__(self):
        return 'The Apex API endpoint could not be reached.'


class ApexDataObjectException(ApexError):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object of type {type(self.object)} could not be retrieved.'


class ApexObjectNotFoundException(ApexError):

    def __init__(self, import_id):
        self.import_id = import_id

    def __str__(self):
        return f'Object bearing ImportId {self.import_id} could not ' \
               'be retrieved.'


class ApexMalformedJsonException(ApexError):

    def __init__(self, obj):
        self.obj = obj

    def __str__(self):
        return 'Received bad JSON response: ' + str(self.obj)


class ApexEmailException(ApexDataObjectException):

    def __str__(self):
        return ('There was an issue with the email address of an '
                'Apex data object.')


class ApexStudentNoEmailException(ApexEmailException):

    def __init__(self, import_user_id):
        self.user_id = import_user_id

    def __str__(self):
        return f'Student with EDUID {self.user_id} has no email.'


class ApexStaffNoEmailException(ApexEmailException):

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'Staff member "{self.name}" does not have an email address.'


class ApexMalformedEmailException(ApexEmailException):

    def __init__(self, import_user_id: Union[str, int], email: str):
        self.user_id = import_user_id
        self.email = email

    def __str__(self):
        return (f'Attempt to create student or staff with ID {self.user_id} '
                'failed ' 'due to an email address that does not '
                'conform to Apex validation rules: ' + self.email)


class ApexMaxPostSizeException(ApexError):

    def __init__(self, max_size):
        self.max_size = max_size

    def __str__(self):
        return ('Attempted to post more than {:,} objects at once.'
                .format(self.max_size))


class ApexAuthenticationError(ApexError):

    def __init__(self, msg: str = None):
        self.msg = msg

    def __str__(self):
        if self.msg is None:
            return 'There was a problem with Apex API authentication.'
        else:
            return self.msg


class ApexBatchTimeoutError(ApexError):

    def __init__(self, batch_token: Union[str, int]):
        self.status_token = int(batch_token)

    def __str__(self):
        return f'Batch processing time for token {self.status_token} ' \
               'exceed the timeout limit.'


class ApexNotAuthorizedError(ApexAuthenticationError):

    def __str__(self):
        return 'Apex token was rejected by the API.'


class NoUserIdException(ApexError):

    def __str__(self):
        return 'Object does not have an ImportUserID.'


class NoProductCodesException(ApexDataObjectException):

    def __init__(self, classroom_id):
        self.classroom_id = classroom_id

    def __str__(self):
        return f'Classroom with ID {self.classroom_id} has no product codes.'


class DuplicateUserException(ApexDataObjectException):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object with user id {self.object.import_user_id} already exists.'


class PSException(Exception):

    def __str__(self):
        return 'An unexpected error occurred when attempting to interface' \
               ' with PowerSchool.'


class PSEmptyQueryException(PSException):

    def __init__(self, url):
        self.url = url

    def __str__(self):
        return f'Query to URL "{self.url}" returned no results.'


class PSNoConnectionError(PSException):

    def __str__(self):
        return 'Could not establish connection with PowerSchool server.'
