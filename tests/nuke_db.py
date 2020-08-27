from os import environ
import logging
import pickle

from apex_synchronizer import adm
from .utils import PICKLE_DIR


def nuke():
	logger = logging.getLogger(__name__)
	is_dev = bool(environ.get('DEV_ENV', 0))
	if not is_dev:
		raise ValueError('Development environment not loaded.')

	enrollment_file = PICKLE_DIR/'ps_enrollment.pickle'
	logger.info('Reading serialized enrollment info.')
	enroll = pickle.load(open(enrollment_file, 'rb'))

	logger.info('Iterating over students and de-enrolling them.')
	n_students = len(enroll.roster)
	for i, eduid in enumerate(enroll.roster):
		prog = f'{i + 1}/{n_students}:{eduid}:'
		student = adm.ApexStudent.from_powerschool()


if __name__ == '__main__':
	nuke()

