import json
import os
import requests
from collections import namedtuple
from requests.auth import HTTPBasicAuth


CLIENT_ID = os.environ['CONSUMER_KEY']
SECRET_KEY = os.environ['SECRET_KEY']
BASE_URL = 'https://api.apexvs.com/'


Student = namedtuple('Student', 'ImportUserId ImportOrgId FirstName \
                                 MiddleName LastName Email Role GradeLevel \
                                 LoginId LoginPw CoachEmails')


class ApexSynchronizer(object):

    #TODO
    def __init__(self):
        self.access_token = 0


def get_token():
    url = BASE_URL + 'token'
    request_json = {
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': SECRET_KEY
    }
    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(CLIENT_ID, SECRET_KEY)
    r = requests.post(url, json=request_json, headers=headers, auth=auth)
    if r.status_code == 200:
        token = r.json()['access_token']
        return token
    else:
        # TODO: Add proper exception handling
        raise Exception('Could not get token')


def get_header(token: str):
    return {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }


def put_students(students: list):
    token = get_token()
    student_json = {'studentUsers': [dict(student._asdict()) for student in students]}

    student_json = json.dumps(student_json)
    header = get_header(token)
    url = BASE_URL + 'students'
    return requests.post(url=url, data=student_json, headers=header) 


def main():
    high_code = 'Z8102253'
    url = BASE_URL + 'products/' + high_code
    #url = BASE_URL + 'students' 
    student = Student(ImportUserId='123456', ImportOrgId=high_code, FirstName='Brandon',
                      MiddleName='Loyal', LastName='Sorensen', Email='sorensen.12@gmail.com',
                      Role='S', GradeLevel=10, LoginId='LoginId', LoginPw='LoginPw',
                      CoachEmails='test@test.com')

                     
    r = put_students([student])
    print(r.text)


if __name__ == '__main__':
    main()

