import mysql.connector
from datetime import datetime
from enum import Enum
import logging
import sys
import json
import yaml

class CounterType(Enum):
    PUSH_COUNT=1
    PULL_COUNT=2
    EMPTY_COUNT=3

#logger
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

#read yml configurations
with open ('./config/config.yml') as f:
    cfg = yaml.load(f, Loader=yaml.FullLoader)
    dbUser = cfg['mysql']['user']
    dbPassword = cfg['mysql']['password']
    dbHost = cfg['mysql']['host']


ROW_ID = 0
databaseName = "amazon-sqs"
PUSH_COLUMN_INDEX = 1
PULL_COLUMN_INDEX = 2
EMPTY_COLUMN_INDEX = 3

selectQuery = ("select * from telemetries where id = %s")
insertQuery = ("insert into telemetries (id, push_count, pull_count, empty_count, LUT) VALUES (%s,%s,%s,%s,%s)")  
updatePushCountQuery = ("update telemetries SET push_count = push_count+1, LUT=%s where id = 0") 
updatePullCountQuery = ("update telemetries SET pull_count = pull_count+1, LUT=%s where id = 0") 
updateEmptyCountQuery = ("update telemetries SET empty_count = empty_count+1, LUT=%s where id = 0")
nullifyCountersQuery = ("update telemetries SET push_count = 0, pull_count = 0, empty_count = 0, LUT=%s where id = 0") 


def createDbConnection():
    db = mysql.connector.connect(
            host=dbHost, 
            user=dbUser, 
            password=dbPassword, 
            database=databaseName
    )

    return db

#check if the table is empty, if so, init it with 0's
# (should happen once in entire lifecycle of project)
def initDbIfNeeded():
    db = createDbConnection()
    #create a cursor object
    cur = db.cursor()
 
    try:  
        #Reading the table data      
        #cur.execute("select * from telemetries where id = %s", (ROW_ID,))
        cur.execute(selectQuery, (ROW_ID,))    
        
        #fetching the rows from the cursor object  
        result = cur.fetchall()  
        #printing the result  

        if  len(result) == 0:
            cur.execute(insertQuery, (ROW_ID, 0,0,0,datetime.now()))  
            db.commit()
            logger.info(f"{cur.rowcount} record(s) affected")
       
    except Exception as e:  
        db.rollback()  
        logger.error(e)
    
    db.close()  




def incrementCounter(counterType):
    if counterType == CounterType.PUSH_COUNT:
        query = updatePushCountQuery
    elif counterType== CounterType.PULL_COUNT:
        query = updatePullCountQuery
    else:
        query = updateEmptyCountQuery
        
    db = createDbConnection()
    cur = db.cursor()
   
    try:  
   
       logger.debug(f"Will update counter type {counterType} with increased country and {datetime.now()}. Query is : {query}")

       #as we use a single SQL query here and it's atomic, no need to place locks (e.g. by SELECT ... FOR UPDATE)
       cur.execute(query, (datetime.now(),))  
       db.commit()

       logger.info(f"{cur.rowcount} record(s) affected")

    except Exception as e:  
        db.rollback()  
        logger.error(e)
    
    db.close()  



def incrementPushCount():
   incrementCounter(CounterType.PUSH_COUNT)


def incrementPullCount():
    incrementCounter(CounterType.PULL_COUNT)


def incrementEmptyCount():
    incrementCounter(CounterType.EMPTY_COUNT)


#get the current telemetries
def getTelemetries():

    db = createDbConnection()
    #create a cursor object
    cur = db.cursor()
   
    try:  
        #Reading the table data      
       cur.execute(selectQuery, (ROW_ID,))  
       
       #fetching the rows from the cursor object  
       result = cur.fetchall()  

       if len(result) != 1:
            logger.exception("Got more than one result while querying telemetries table")
            return
      
       resultRow = result[0]

       pushCount = resultRow[PUSH_COLUMN_INDEX]
       pullCount = resultRow[PULL_COLUMN_INDEX]
       emptyCount = resultRow[EMPTY_COLUMN_INDEX]

       #now build the json
       resultDict = {
           "push" : pushCount,
           "pull" : pullCount,
           "empty_queue" : emptyCount
       }

       jsonString = json.dumps(resultDict)
       

    except Exception as e:  
        db.rollback()  
        print(e)
    
    db.close()  
    return jsonString



def nullifyTelemetries():

    db = createDbConnection()
    #create a cursor object
    cur = db.cursor()
   
    try:  

       logger.debug(f"Nullifying all counters with date = {datetime.now()}. Query is : {nullifyCountersQuery}")

       cur.execute(nullifyCountersQuery, (datetime.now(),))  
       db.commit()

       logger.info(f"{cur.rowcount} record(s) affected")

    except Exception as e:  
        db.rollback()  
        logger.error(e)
    
    db.close()  




#nullifyTelemetries()
#incrementPullCount()
#incrementPushCount()
#incrementEmptyCount()
#print(getTelemetries())

