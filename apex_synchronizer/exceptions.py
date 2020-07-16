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
        return f'Object bearing ImportId {self.import_id} could not be retrieved.'


class ApexMalformedJsonException(ApexError):

    def __init__(self, obj):
        self.obj = obj

    def __str__(self):
        return 'Received bad JSON response: ' + str(self.obj)


class ApexNoEmailException(ApexError):

    def __init__(self, import_user_id):
        self.user_id = import_user_id

    def __str__(self):
        return f'Student with EDUID {self.user_id} has no email.'


class ApexMalformedEmailException(ApexError):

    def __init__(self, import_user_id: Union[str, int], email: str):
        self.user_id = import_user_id
        self.email = email

    def __str__(self):
        return (f'Attempt to create student with EDUID {self.user_id} '
                'failed ' 'due to an email address that does not '
                'conform to Apex validation rules: ' + self.email)


class NoUserIdException(ApexDataObjectException):

    def __str__(self):
        return 'Object does not have an ImportUserID.'


class DuplicateUserException(ApexDataObjectException):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object with user id {self.object.import_user_id} already exists.'
