import logging.config
import os

path = 'conf/log.conf'

logging.config.fileConfig(path)
logger_operation = logging.getLogger('operation')
logger_run = logging.getLogger('run')
logger_security = logging.getLogger('security')
logger_user = logging.getLogger('user')
logger_interface = logging.getLogger('interface')
