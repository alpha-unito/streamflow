import logging

dateFormat = '%(asctime)s'
levelFormat = ' %(levelname)-8s'
msgFormat = '%(message)s'

class CustomFormatter(logging.Formatter):
    """
    Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629
    """

    grey = "\x1b[38;20m"
    blue = "\x1b[34m"
    bold_blue = "\x1b[1;34m"
    yellow = "\x1b[33;20m"
    bold_yellow = "\x1b[33;1m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: blue + dateFormat + reset + levelFormat + msgFormat + reset,
        logging.INFO: blue + dateFormat + bold_blue + levelFormat + reset + msgFormat + reset,
        logging.WARNING: yellow + dateFormat + bold_yellow + levelFormat + reset + yellow + msgFormat + reset,
        logging.ERROR: red + dateFormat + bold_red + levelFormat + reset + red + msgFormat + reset,
        logging.CRITICAL: red + dateFormat + reset + bold_red + levelFormat + msgFormat + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

class HighlitingFilter(logging.Filter):
    bold_cyan = "\x1b[1;36m"
    reset = "\x1b[0m"

    def __init__(self):
        super(HighlitingFilter, self).__init__()
        self._patterns = ["CANCELLED"]

    def filter(self, record):
        record.msg = self.highlight(record.msg)
        return True

    def highlight(self, msg):
        msg = str(msg)
        for pattern in self._patterns:
               msg = msg.replace(pattern, self.bold_cyan + pattern + self.reset)
        return msg

logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
defaultStreamHandler.setFormatter(formatter)
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False
