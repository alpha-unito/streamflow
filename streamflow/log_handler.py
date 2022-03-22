import logging

logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
defaultStreamHandler.setFormatter(formatter)
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False
