#!/usr/bin/env python3

from os.path import join
from pathlib import Path
from typing import Collection, Union
import argparse
import csv
import json
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
FIELDS = [
    'ActivityCountComplete',
    'ActivityCountCompleteDueToNow',
    'ActivityCountDueToNow',
    'ActivityCountTotal',
    'CourseName',
    'DateOfLastActivity',
    'EDUID',
    'ExtraCredit',
    'FinalGrade',
    'FirstName',
    'GradeToDate',
    'ImportUserId',
    'LastAccessDate',
    'LastName',
    'OnScheduleIndicator',
    'OnSchedulePercent',
    'OverallPercent',
    'OverdueActivities',
    'ProductCode',
    'QualityOfWork',
    'StudentStartDate',
    'TotalPtsAttempted',
    'TotalPtsCompleted',
    'TotalPtsEarnedOnActivitiesDueToNow',
    'TotalPtsPossible',
    'TotalPtsPossibleDueToNow',
    'TotalSessionTimeInMinutes'
]

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--use-pickle', default=False, nargs='?',
                        help='use a serialized ApexEnroll, defaulting '
                             'to "./serial/apex_enroll.pickle"')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Show TQDM status bar.')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='activate debug features')
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


def write_reports_to_csv(reports: Collection):
    logger = logging.getLogger(__name__)
    student2eduid = get_eduid_map()
    out = open('reports.csv', 'w+')
    writer = csv.DictWriter(out, delimiter=',', fieldnames=FIELDS)
    writer.writeheader()

    for class_reports in reports:
        for student_report in class_reports:
            s_id = student_report['ImportUserId']
            try:
                student_report['EDUID'] = student2eduid[s_id]
            except KeyError:
                logger.debug(f'Student bearing ID "{s_id}" not found in '
                             'PS database')
                continue

            writer.writerow(student_report)

    out.close()


def main():
    args = parse_args()
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    if args.debug:
        reports: list = json.load(open('serial/all_reports.json'))
        write_reports_to_csv(reports)
        return

    session = ApexSession()
    apex_enroll = get_enrollment(args.use_pickle, session)
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
        logger.debug('Recieved report: ' + str(report))
        if report != [{}]:
            all_reports.append(report)

    json.dump(all_reports, open(join('serial', 'all_reports.json'), 'w+'))
    write_reports_to_csv(all_reports)


if __name__ == '__main__':
    main()
