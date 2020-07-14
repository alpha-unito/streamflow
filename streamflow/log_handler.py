import logging

logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False
