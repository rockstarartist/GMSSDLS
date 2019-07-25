import os,sys
import json
from kafka import KafkaConsumer
import logging
import datetime

#Prepare Logging for Service
logger = logging.getLogger('ISBN_Lookup_Service_Logs')
stderr_log_handler = logging.StreamHandler()
logger.addHandler(stderr_log_handler)

#Format log output
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stderr_log_handler.setFormatter(formatter)

#Set & Get Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL","INFO")
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS","kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC","t.messagebus")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID","isbn_lookup_service_consumer_group")
KAFKA_MAX_POLL_RECORDS = os.environ.get("KAFKA_MAX_POLL_RECORDS", 10000)
KAFKAHEADER_SERVICE_KEY = os.environ.get("KAFKAHEADER_SERVICE_KEY","servicetype")
KAFKAHEADER_ACTION_KEY = os.environ.get("KAFKAHEADER_ACTION_KEY","serviceaction")
THIS_SERVICE_TYPE = "isbn_lookup"

#Set Logger
logger.setLevel(LOG_LEVEL)
logger.info('Environment Values: %s %s %s %s'%(KAFKA_BROKERS,KAFKA_TOPIC,KAFKA_GROUP_ID) )

#Actions that this service can peform and default parameters
valid_actions_dict = {
    "getISBNInformation": {"param1": "param1value","param2": "param2value"}
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

                    ### TODO: Perform ISBN Lookup and transformation actions
                    logger.info("No Operation. Functionality has not been developed.")
                #Incoming messages have headers, but they mean nothing to this consumer
                #Ignore these messages.
                else:
                    pass
        except:
            logger.info("Unexpected error: %s"%(sys.exc_info()[0]))
            pass

def startApplication():
    logger.info("starting isbn-lookup-service consumer")
    consumer = KafkaConsumer(KAFKA_TOPIC, group_id=KAFKA_GROUP_ID, max_poll_records=KAFKA_MAX_POLL_RECORDS, bootstrap_servers=[KAFKA_BROKERS],api_version=(1, 1, 0))

startApplication()
