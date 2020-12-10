import logging
import sys
import requests
import json 
from datetime import datetime

host = "3.122.91.219" 
#host = "127.0.0.1"
port = "5000"

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def callPush(UUID, ts, message):
    url = "http://"+host+ ":" +port +"/api/push"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    data = { 'uuid' : UUID , 'ts' : ts , 'msg' : message}
    dataJson = json.dumps(data)
    response = requests.post(url, dataJson, headers=headers)
    logger.info(response)
    return response



def callPull():
    url = "http://"+host+ ":" +port +"/api/pull"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.get(url,headers=headers)
    logger.info(response.text)
    return response



def callGetTelemetries():
    url = "http://"+host+ ":" +port +"/api/telemetries"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    response = requests.get(url,headers=headers)
    logger.info(response.text)
    return response


def pushMessages(messageCount):
    logger.info(f"Calling push messages with count {messageCount}")
    if messageCount > 100:
        logger.error('messageCount should be < 100')
        return

    for x in range (0,messageCount):
        UUID = f"UUID{x}"
        date = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        message = f"message{x}"
        callPush(UUID, date, message)


def pullMessages(messageCount):
    logger.info(f"Calling pull messages with count {messageCount}")
    if messageCount > 100:
        logger.error('messageCount should be < 100')
        return

    for x in range (0,messageCount):
       callPull()


#After each such calls we expect the telemteries to rise by: push: +10, pull: +10, empty: +5
pushMessages(10)
pullMessages(15)
callGetTelemetries()
#callPush('myUUD', 'myTS', 'myMessage')
#callPull()
#callGetTelemetries()