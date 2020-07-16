import bz2
import json
import logging
import os
import re
from datetime import datetime
from typing import BinaryIO

"""
This script is useful for checkpointing log files. Because the `apex_synchronizer`
module makes such extensive use of Python's logging library, the log files it produces
will get quite large. However, given that much of the content, such as the repeated
time stamps, are highly compressible, this script is to be run every so often to
preserve the information in the log files in a much more space efficient manner.

The script works by compressing the files with BZip2, then storing those files
in a folder within the directory specified by the `LOGDIR` environment variable
(defaulting to the current working directory if none is specified). The files
are named in the following format: "{first day}_{last day}". First day and last
day refer to the dates on the first and last log entries, respectively. They are
in the format YYYY-MM-DD so that they are lexigraphically sortable.
"""

DEFAULT_FILE_NAMES = 'apex_api.log', 'error.log', 'powerschool.log'
LOG_DT_FMT = '%Y-%m-%d %H:%M:%S.%f'
OUTPUT_DT_FMT = '%Y-%m-%d'
DATE_REG = re.compile(r'^202\d-[01]\d-[0-3]\d [0-2]\d:[0-6]\d:\d{,10}\.\d{3}')


def get_file_names(config_file: str = None):
    if config_file is None:
        config_file = os.path.join(os.getcwd(), 'logging_config.json')

    try:
        config = json.load(open(config_file, 'r'))
    except FileNotFoundError:
        return DEFAULT_FILE_NAMES

    return [handler['filename'] for handler in config['handlers'].values()
            if handler['class'] == 'logging.FileHandler']


def compress_files(log_dir, delete_files=True):
    logger = logging.getLogger(__name__)
    file_names = get_file_names(os.environ.get('LOG_CONFIG'))
    for log_file_name in file_names:
        log_file = os.path.join(log_dir, log_file_name)
        if not os.path.exists(log_file) or not os.stat(log_file).st_size:
            logger.exception(log_file_name + ' is empty or doesn\'t exist.')
            if delete_files:
                os.remove(log_file)
            continue

        with open(log_file, 'rb') as data:
            try:
                first_date = extract_date(get_first_line(data))
                last_date = extract_date(get_last_line(data))
            except ValueError:
                logger.exception('Date not found in file: ' + log_file_name)
                continue

            bz2_contents = bz2.compress(data.read())

        output_date = first_date + '_' + last_date
        output_name = output_date + '_' + log_file_name + '.bz2'
        output_dir = os.path.join(log_dir, 'checkpoints')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_name)

        with bz2.open(output_path, 'wb') as bz_file:
            bz_file.write(bz2_contents)

        if delete_files:
            os.remove(log_file)


def get_first_line(f: BinaryIO) -> str:
    return f.readline().decode()


def get_last_line(f: BinaryIO) -> str:
    """https://stackoverflow.com/questions/46258499/read-the-last-line-of-a-file-in-python"""
    f.seek(-2, os.SEEK_END)
    while f.read(1) != b'\n':
        f.seek(-2, os.SEEK_CUR)
    return f.readline().decode()


def extract_date(s: str) -> str:
    date_match = re.match(DATE_REG, s)
    if not date_match:
        raise ValueError('No date in file.')
    date_str = date_match.group(0)
    date_str = datetime.strptime(date_str, LOG_DT_FMT)
    return date_str.strftime(OUTPUT_DT_FMT)


if __name__ == '__main__':
    compress_files(log_dir=os.environ.get('LOGDIR', 'log_files'))
