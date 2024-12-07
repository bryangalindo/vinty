import logging


def get_logger(name):
    log_format = (
        "%(asctime)s [%(levelname)s] %(module)s:%(funcName)s:%(lineno)d - %(message)s"
    )
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger(name)
    return log
