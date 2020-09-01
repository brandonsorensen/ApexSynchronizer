#!/usr/bin/env python3
from logging.config import dictConfig
from typing import Union
import logging
import os
import json

from apex_synchronizer import ApexSynchronizer, ApexSchedule


def setup_logging(config_file: str = None, log_dir: str = None,
                  log_level: Union[str, int] = None) -> dict:
    if config_file is None:
        config_file = os.path.join(os.getcwd(), 'logging_config.json')

    if log_level is None:
        log_level = os.environ.get('LOGLEVEL', logging.INFO)

    config = json.load(open(config_file, 'r'))

    for obj_type in 'loggers', 'handlers':
        obj: dict
        for obj in config[obj_type].values():
            if obj['level'] in ('NOTSET', logging.NOTSET):
                # Use environment variable if level is not set
                obj['level'] = log_level
            if (obj_type == 'handlers'
                    and log_dir is not None
                    and 'filename' in obj.keys()):
                obj['filename'] = os.path.join(log_dir, obj['filename'])

    return config


def main():
    logging_config = setup_logging(log_dir=os.environ.get('LOGDIR'))
    dictConfig(logging_config)

    schedule_path = os.environ.get('SCHEDULE_PATH', 'apex_schedule.json')
    try:
        schedule = ApexSchedule.from_json(schedule_path)
    except FileNotFoundError:
        schedule = ApexSchedule.default()

    sync_agent = ApexSynchronizer()
    sync_agent.run_schedule(schedule)


if __name__ == '__main__':
    main()
