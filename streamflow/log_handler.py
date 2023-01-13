import logging

dateFormat = "%(asctime)s"
levelFormat = " %(levelname)-8s"
msgFormat = "%(message)s"


class CustomFormatter(logging.Formatter):
    """
    Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629
    """

    grey = "\x1b[38;20m"
    green = "\x1b[32m"
    bold_green = "\x1b[1;32m"
    yellow = "\x1b[33;20m"
    bold_yellow = "\x1b[33;1m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: green + dateFormat + reset + levelFormat + msgFormat + reset,
        logging.INFO: green
        + dateFormat
        + bold_green
        + levelFormat
        + reset
        + msgFormat
        + reset,
        logging.WARNING: yellow
        + dateFormat
        + bold_yellow
        + levelFormat
        + reset
        + yellow
        + msgFormat
        + reset,
        logging.ERROR: red
        + dateFormat
        + bold_red
        + levelFormat
        + reset
        + red
        + msgFormat
        + reset,
        logging.CRITICAL: red
        + dateFormat
        + reset
        + bold_red
        + levelFormat
        + msgFormat
        + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class HighlitingFilter(logging.Filter):
    bold_green = "\x1b[1;32m"
    bold_yellow = "\x1b[33;1m"
    bold_red = "\x1b[31;1m"
    bold_blue = "\x1b[1;34m"
    red = "\x1b[31;20m"
    reset = "\x1b[0m"

    patterns = {
        "CANCELLED": 3,  # bad
        "SKIPPED": 2,  # less bad
        "COMPLETED": 1,  # good
        "FAILED": 3,
        "EXECUTING": 0,  # status report messages
        "COPYING": 0,
        "UNDEPLOYING": 0,  # put before deploying as one word is part of the other,
        # otherwise it would highlight only a part of the word
        "DEPLOYING": 0,
        "SCHEDULED": 0,
    }

    def __init__(self):
        super().__init__()

    def filter(self, record):
        record.msg = self.highlight(record.msg)
        return True

    def highlight(self, msg):
        msg = str(msg)
        msg_tok = msg.split()
        for pattern, category in self.patterns.items():
            if msg_tok[0] == pattern:
                if category == 0:
                    msg_tok[0] = msg_tok[0].replace(
                        pattern, self.bold_blue + pattern + self.reset
                    )
                elif category == 1:
                    msg_tok[0] = msg_tok[0].replace(
                        pattern, self.bold_green + pattern + self.reset
                    )
                elif category == 2:
                    msg_tok[0] = msg_tok[0].replace(
                        pattern, self.bold_yellow + pattern + self.reset
                    )
                elif category == 3:
                    # Failed workflows are reported as error-level logging, hence the coloring here
                    # should comply with the coloring in the logger formatter: plain red formatting
                    # is restored after the bold red error token
                    msg_tok[0] = msg_tok[0].replace(
                        pattern, self.bold_red + pattern + self.reset + self.red
                    )
        return " ".join(msg_tok)


logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
formatter = logging.Formatter(
    fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
defaultStreamHandler.setFormatter(formatter)
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False
