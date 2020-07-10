import logging
import os
import json
from apex_synchronizer import ApexSynchronizer
from logging.config import dictConfig


def setup_logging(config_file: str = None, log_dir: str = None) -> dict:
    if config_file is None:
        config_file = os.path.join(os.getcwd(), 'logging_config.json')

    config = json.load(open(config_file, 'r'))

    for obj_type in 'loggers', 'handlers':
        obj: dict
        for obj in config[obj_type].values():
            if obj['level'] in ('NOTSET', logging.NOTSET):
                # Use environment variable if level is not set
                obj['level'] = os.environ.get('LOGLEVEL', logging.INFO)
            if (obj_type == 'handlers'
                    and log_dir is not None
                    and 'filename' in obj.keys()):
                obj['filename'] = os.path.join(log_dir, obj['filename'])

    return config


def main():
    logging_config = setup_logging(log_dir=os.environ.get('LOGDIR', None))
    dictConfig(logging_config)
    sync_agent = ApexSynchronizer()
    sync_agent.sync_rosters()


if __name__ == '__main__':
    main()
