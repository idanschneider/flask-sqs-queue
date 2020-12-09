import mysql_client
import logging
import sys


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def increase_empty_queue():
    logger.debug("increase_empty_queue")
    mysql_client.incrementEmptyCount()



def increase_push():
    logger.debug("increase_push")
    mysql_client.incrementPushCount()


def increase_pull():
    logger.debug("increase_pull")
    mysql_client.incrementPullCount()


#get telemetries from DB and return JSON as follows:
#{
#	push: X,
#	pull: Y,
#	empty_queue: Z
#} 
def get_telemetries():
    logger.debug("get_telemetries")
    return mysql_client.getTelemetries()




def nullify_telemetries():
    logger.debug("nullify_telemetries")
    return mysql_client.nullifyTelemetries()