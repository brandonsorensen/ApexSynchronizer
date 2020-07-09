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


class NoUserIdException(ApexDataObjectException):

    def __str__(self):
        return 'Object does not have an ImportUserID.'


class DuplicateUserException(ApexDataObjectException):

    def __init__(self, obj):
        self.object = obj

    def __str__(self):
        return f'Object with user id {self.object.import_user_id} already exists.'
