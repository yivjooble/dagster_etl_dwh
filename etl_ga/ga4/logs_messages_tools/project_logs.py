import logging
from logging.handlers import TimedRotatingFileHandler


def get_logger(name, log_dir):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(log_dir, when='W0', interval=1, backupCount=4)
    handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s:%(message)s'))
    logger.addHandler(handler)

    return logger
