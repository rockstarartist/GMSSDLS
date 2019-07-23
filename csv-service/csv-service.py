import os,sys
import json
from kafka import KafkaConsumer
import logging
import datetime

#Prepare Logging for Service
logger = logging.getLogger('CSV_Service_Logs')
stderr_log_handler = logging.StreamHandler()
logger.addHandler(stderr_log_handler)

#Format log output
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stderr_log_handler.setFormatter(formatter)

#Set & Get Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL","INFO")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS","kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC","t.messagebus")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID","csv_service_consumer_group")
KAFKA_MAX_POLL_RECORDS = os.environ.get("KAFKA_MAX_POLL_RECORDS", 10000)
KAFKAHEADER_SERVICE_KEY = os.environ.get("KAFKAHEADER_SERVICE_KEY","servicetype")
KAFKAHEADER_ACTION_KEY = os.environ.get("KAFKAHEADER_ACTION_KEY","serviceaction")
THIS_SERVICE_TYPE = "csv"

#Set Logger
logger.setLevel(LOG_LEVEL)
logger.info('Environment Values: %s %s %s %s'%(KAFKA_BROKERS,KAFKA_TOPIC,KAFKA_GROUP_ID) )

#Actions that this service can peform and default parameters
valid_actions_dict = {
    "csvToJson": {"param1": "param1value","param2": "param2value"},
    "jsonToCsv": {"param1": "param1value","param2": "param2value"}
}

def consumeKafkaData(consumer):
    logger.info("inside consumer")
    logger.info(consumer)
    for msg in consumer:
        #If there are no headers, we are ignoring these messages, they are invalid
        try:
            if len(msg.headers) > 0:
                # Get Kafka headers
                headerdictionary = dict(msg.headers)
                #Incoming Messages Have their index and document type already assigned
                if (KAFKAHEADER_SERVICE_KEY and KAFKAHEADER_ACTION_KEY in headerdictionary) and (KAFKAHEADER_SERVICE_KEY == THIS_SERVICE_TYPE) and (KAFKAHEADER_ACTION_KEY in valid_actions_dict):

                    ### TODO: Perform CSV transformation actions

                #Incoming messages have headers, but they mean nothing to this consumer
                #Ignore these messages.
                else:
                    pass
        except:
            logger.info("Unexpected error: %s"%(sys.exc_info()[0]))
            pass

def startApplication():
    logger.info("starting csv-service consumer")
    consumer = KafkaConsumer(KAFKA_TOPIC, group_id=KAFKA_GROUP_ID, max_poll_records=KAFKA_MAX_POLL_RECORDS, bootstrap_servers=[KAFKA_BROKERS],api_version=(1, 1, 0))

startApplication()
