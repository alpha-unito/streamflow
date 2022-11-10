import logging

logFormat = '%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s'
dateFormat = '%Y-%m-%d %H:%M:%S'

logger = logging.getLogger("streamflow")
defaultStreamHandler = logging.StreamHandler()
formatter = logging.Formatter(fmt=logFormat,
                              datefmt=dateFormat)
defaultStreamHandler.setFormatter(formatter)
logger.addHandler(defaultStreamHandler)
logger.setLevel(logging.INFO)
logger.propagate = False
