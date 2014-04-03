'''
Created on Oct 13, 2013

@author: hxr
'''
import logging
import logging.handlers
import os
from ConfigLoader import *

logger = logging.getLogger(__name__);
logger.setLevel(logging.DEBUG);

if os.path.exists(logConfig.logFolder+logConfig.logFileName):
    os.remove(logConfig.logFolder+logConfig.logFileName)

handler = logging.handlers.RotatingFileHandler(logConfig.logFolder+logConfig.logFileName, maxBytes=20000000, backupCount=10);
handler.setLevel(logging.INFO);

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)
# logging.getLogger().addHandler(logging.StreamHandler())
