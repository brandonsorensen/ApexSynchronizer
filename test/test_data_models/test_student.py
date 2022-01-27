from copy import deepcopy
import unittest
from urllib.parse import urljoin

import json
import responses

from apex_synchronizer import adm, ApexSession, exceptions
from .constants import DATA_DIR


class TestStudentResources(unittest.TestCase):

    url = adm.ApexStudent.url + '/'
    fixture_types = [
        'student_list',
        'student_detail'
    ]

    def setUp(self):
        self.session = ApexSession()
        self.responses = responses.RequestsMock()
        self.responses.start()
        self.fixtures = {}
        for fixture_type in self.fixture_types:
            file_name = DATA_DIR/f'apex_{fixture_type}.json'
            with open(file_name, 'r') as f:
                self.fixtures[fixture_type] = json.load(f)

        self.addCleanup(self.responses.stop)
        self.addCleanup(self.responses.reset)

    def test_get_detail_valid(self):
        test_id = 'test@test-email.com'
        self.responses.add(
            responses.Response(
                responses.GET, urljoin(self.url, test_id),
                status=200,
                content_type='application/json',
                body=self.student_detail
            )
        )
        s = adm.ApexStudent.get(test_id, session=self.session)
        self.assertEqual(s.import_user_id, test_id)

    @property
    def student_detail(self):
        return json.dumps(self.fixtures['student_detail']['test_base'])


class TestStudentConstructor(unittest.TestCase):

    def test_bad_email(self):
        """Tests email-related cases for constructor."""
        kwargs = self.get_test_kwargs()
        kwargs['email'] = None
        with self.assertRaises(exceptions.ApexNoEmailException):
            adm.ApexStudent(**kwargs)
            kwargs['email'] = ''
            adm.ApexStudent(**kwargs)

        kwargs['email'] = 'invalid email address'
        with self.assertRaises(exceptions.ApexMalformedEmailException):
            adm.ApexStudent(**kwargs)

    def get_test_kwargs(self) -> dict:
        return deepcopy(self.fixtures['student_detail']['test_kwargs'])


if __name__ == '__main__':
    unittest.main()