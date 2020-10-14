from pathlib import Path
from typing import Union
import argparse
import logging
import pickle

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    def tqdm(x): return x

from apex_synchronizer import adm, ApexSession
from apex_synchronizer.enrollment import ApexEnrollment
from apex_synchronizer.ps_agent import get_eduid_map

DEFAULT_PICKLE = Path('serial')/'apex_enroll.pickle'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--use-pickle', default=False, nargs='?',
                        help='use a serialized ApexEnroll, defaulting '
                             'to "./serial/apex_enroll.pickle"')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show TQDM status bar.')
    return parser.parse_args()


def get_enrollment(use_pickle: Union[bool, str],
                   session: ApexSession = None) -> ApexEnrollment:
    if use_pickle is False:
        # -p flag was not given
        return ApexEnrollment(session=session)
    elif use_pickle is None:
        # -p arg/flag was supplied with no directory
        return pickle.load(open(DEFAULT_PICKLE, 'rb'))
    else:
        # -p was given and an argument was supplied
        return pickle.load(open(use_pickle, 'rb'))


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    session = ApexSession()
    args = parse_args()

    apex_enroll = get_enrollment(args.use_pickle, session)

    student2eduid = get_eduid_map()
    cr_index = 1
    n_classrooms = len(apex_enroll.classrooms)

    classrooms = (tqdm(apex_enroll.classrooms) if args.verbose
                  else apex_enroll.classrooms)

    all_reports = []
    for classroom in classrooms:
        logger.debug(f'{cr_index}/{n_classrooms}:'
                     f'{classroom.import_classroom_id}:'
                     'Getting reports')
        report = classroom.get_reports(session=session)
        if report != [{}]:
            all_reports.append(report)
    print(all_reports)


if __name__ == '__main__':
    main()
