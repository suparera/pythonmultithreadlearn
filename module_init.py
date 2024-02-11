import logging.config
import os.path
import sys

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        #'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s.%(funcName)s():%(lineno)d %(message)s'}
        'standard': {'format': '%(message)s'}
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            "stream": "ext://sys.stdout"
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': os.path.join('logs', 'app.log'),
            'mode': 'w',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}
logging.config.dictConfig(logging_config)

def logger():
    # Get the name from the caller of this function
    return logging.getLogger(sys._getframe(1).f_globals['__name__'])