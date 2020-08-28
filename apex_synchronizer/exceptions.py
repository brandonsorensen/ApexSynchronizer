from typing import List, Union

from requests import Response
from requests.exceptions import RequestException


class ApexError(Exception):

    def __init__(self, e=None):
        self.error = e

    def __str__(self):
        out = 'There was an error when interfacing with the Apex API'
        if self.error is None:
            return out + '.'
        else:
            return f'{out}:\n{self.error}'


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


class ApexNoEnrollmentsError(ApexError):

    """
    For use when attempting to fetch enrollments for a student who is
    enrolled in a school in Apex but not in any classrooms.
    """

    def __init__(self, import_id):
        self.import_id = import_id

    def __str__(self):
        return f'Student bearing EDUID "{self.import_id}" is ' \
               'enrolled in Apex but has not enrollments.'


class ApexUnrecognizedOrganizationError(ApexError):
    """Used when an org_id is not one of the recognized Oneida orgs."""
    def __init__(self, org: Union[int, str]):
        self.org = org

    def __str__(self):
        return f'{self.org} is not a recognizaed organization.'


class ApexIncompleteOperationError(ApexError):
    """
    To be raised when an operation that requires multiple calls to the
    Apex API does finish all of those calls.
    """
    def __init__(self, error: RequestException, responses: List[Response]):
        self.error = error
        self.responses = responses
        self.n_success = len(responses)

    def __str__(self):
        return ('Operation could not complete. Received the following error '
                f'after {self.n_success} successful sub-operations:'
                f'\n{self.error}')


class ApexMalformedJsonException(ApexError):

    def __init__(self, json_obj):
        self.obj = json_obj

    def __str__(self):
        return 'Received bad JSON response: ' + str(self.obj)


class ApexNoChangeSubmitted(ApexError):

    """
    To be raised when a method that returns a response, for whatever
    reason, is not able to submit the request to Apex.
    """

    def __str__(self):
        return 'No changes were submitted to the Apex server.'


class ApexEmailException(ApexDataObjectException):

    def __str__(self):
        return ('There was an issue with the email address of an '
                'Apex data object.')


class ApexDatetimeException(ApexError):
    """Raised when a date is not given in the correct format."""
    def __init__(self, date: str):
        self.date = date

    def __str__(self):
        return 'The following date is not in the correct format: ' + self.date


class ApexNoEmailException(ApexEmailException):

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f'User "{self.name}" does not have an email address.'


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


class ApexIncompleteDataException(ApexError):
    """
    When JSON objects fetched from the Apex API cannot be used to
    create new `ApexDataObject` objects.
    """
    def __init__(self, json_obj: dict):
        self.json_obj = json_obj

    def __str__(self):
        return 'Received the following incomplete JSON object from ' \
               f'Apex:\n{self.json_obj}'


class ApexNoTeacherException(ApexIncompleteDataException):
    """
    To be raised when a class fetched from the Apex API does not have a
    primary teacher.
    """
    def __str__(self):
        return f'A JSON object returned from Apex is missing a primary ' \
               f'teacher: ' + str(self.json_obj)


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
