from flask import Flask,jsonify,request,render_template
import json
import sqs_client
import mysql_client
import telemetries
import sys #for logging to stdout
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

app = Flask(__name__)


queue_name = 'messages-std-sqs-01'
queue = sqs_client.get_queue(queue_name)


#make sure the database table has a row, if not, create it
logger.debug("Verifying telemetries table is not empty")
mysql_client.initDbIfNeeded()


#Push method - inserts message in SQS and increment push_count in database by 1
@app.route('/api/push' , methods=['POST'])
def push_sqs():
  request_data = request.get_json()

  message_body = json.dumps(request_data) 
  sqs_response = sqs_client.send_message(queue, message_body)
  logger.debug(f"Push to SQS complete. Response is: {sqs_response}")
  logger.debug("Increasing push count")
  telemetries.increase_push() #successful push requesst
  
  return sqs_response
  


#Pull method - reads+removes message from SQS and increment pull_count in database by 1
#If message was read succsfully, returns the message body
#Otherwise, returns an empty json message (as per project requiremnt) + HTTP status 204
@app.route('/api/pull' , methods=['GET'])
def pull_sqs():
  message = sqs_client.receive_message(queue)

  if (message==None):
    logger.debug("Increasing empty queue count")
    telemetries.increase_empty_queue()
    #since nothing was found, returning HTTP Status - no content (204) and an empty message (as per project requirement)
    return '', 204


  #else...
  logger.debug("Increasing pull count")
  telemetries.increase_pull() #successful pull request
  #message is of type sqs.Message. See attributes in https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
  #These are the resource's available attributes:
  #attributes
  #body
  #md5_of_body
  #md5_of_message_attributes
  #message_attributes
  #message_id
  #messageBody = message.body
  messageBody = message.body
  logger.debug("Deleting the message after processing it")
  message.delete()  #delete the message so it won't be processed again
  return messageBody,200,{'content-type':'application/json'}
  
  
#Get the telemetries in JSON format of {push:X, pull:Y, empty_queue:Z}
@app.route('/api/telemetries' , methods=['GET'])
def get_telemetries():
  logger.debug("Getting telemetries")
  telemetriesResponse = telemetries.get_telemetries()  
  return telemetriesResponse,200,{'content-type':'application/json'}





################################################################
#Not part of the exercise, but...
#Added a clear telemetries which sets all telemeteries to 0
#TODO: DELETE THIS (this is just for testing purposes)
@app.route('/api/telemetries' , methods=['DELETE'])
def delete_telemetries():

  telemetries.nullify_telemetries()
  responseJson = '{ "message" : "Telemetries nullfied" }'
  return responseJson,200,{'content-type':'application/json'}


################################################################
  



#Run flask on port 5000
#app.run(host='0.0.0.0')  is required to make app visible outside of local machine.
#See: https://stackoverflow.com/questions/30554702/cant-connect-to-flask-web-service-connection-refused
#Specifically, do NOT use: app.run(port=5000)
app.run(host='0.0.0.0') 

