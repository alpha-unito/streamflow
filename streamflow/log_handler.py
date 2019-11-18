import logging

_logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
_logger.addHandler(defaultStreamHandler)
_logger.setLevel(logging.INFO)
