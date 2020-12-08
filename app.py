from flask import Flask,jsonify,request,render_template
import json
import sqs_client
import telemetries
import sys #for logging to stdout
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

app = Flask(__name__)


queue_name = 'messages-std-sqs-01'
queue = sqs_client.get_queue(queue_name)

@app.route('/api/push' , methods=['POST'])
def push_sqs():
  request_data = request.get_json()

  message_body = json.dumps(request_data) 
  sqs_response = sqs_client.send_message(queue, message_body)

  telemetries.increase_push() #successful push requesst
  
  return sqs_response
  #return jsonify(request_data)



@app.route('/api/pull' , methods=['GET'])
def pull_sqs():
  message = sqs_client.receive_message(queue)

  if (message==None):
    telemetries.increase_empty_queue()
    #since nothing was found, returning HTTP Status - no content (204)
    return '', 204


  #else...
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
  message.delete()  #delete the message so it won't be processed again
  return messageBody
  
  

@app.route('/api/telemetries' , methods=['GET'])
def get_telemetries():

  telemetriesResponse = telemetries.get_telemetries()  
  return jsonify(telemetriesResponse)





################################################################
#Not part of the exercise, but...
#Added a clear telemetries which sets all telemeteries to 0
#TODO: DELETE THIS (this is just for testing purposes)
@app.route('/api/telemetries' , methods=['DELETE'])
def delete_telemetries():

  telemetries.nullify_telemetries()
  return "Telemetries nullified"


################################################################
  



#Run flask on port 5000 (TODO: make this configurable)
app.run(port=5000)
