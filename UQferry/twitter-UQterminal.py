#!/usr/local/bin/python2.7

import sys
import mosquitto
from twitter import *
import logging
import logging.handlers
import json

TOPIC_STATUS = "/uq/ferry/status"
LOG_FILENAME = '/tmp/ferry_twitter.log'
MQTT_SERVER = "winter.ceit.uq.edu.au"
HASHTAG = "#CityCat"

## Set up the logging file(s)
logger = logging.getLogger("ferry")
logger.setLevel(logging.DEBUG)

# create file handler
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000*1024, backupCount=5)

#craete a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handler to the logger
logger.addHandler(handler)


# Decode the incoming message (in JSON) and prepare the text string for twitter.
def on_message(mosq, obj, msg):
    s = str(msg.payload)
    logger.debug("%s: %s", msg.topic, s)
    data = json.loads(s)
    ferry_name = data["vesselName"]
    status = data["status"]
    timing = data["time"]

    is_has = "has" if status in ("docked", "departed") else "is"
    where = "at UQ" if status == "docked" else "UQ"
    tweet_message = 'The ferry "%s" %s %s %s at %s %s' % (ferry_name, is_has, status, where, timing, HASHTAG)
    logger.debug("twitter message:: %s", tweet_message)

#   Now, post the ferry status update
    try:
        t.statuses.update( status=tweet_message)
        logger.info("tweeted: %s", tweet_message)
    except Exception:
        print 'Duplicate tweet dumped'
        logger.error('Duplicate tweet dumped: %s', tweet_message)

def on_connect(mosq, obj, msg):
    # Subscribe to the topic each time we connect - for safety.
    mqttc.subscribe(TOPIC_STATUS, 0)
    logger.info("Successfully (re)subscribed to topic '%s'", TOPIC_STATUS)

##
## Here we go - the main loop
##

# Open up a link to the UQferry twitter account
try:
    t = Twitter( auth=OAuth("1242635330-1aliT4x7gqiDS6Scvvd2n3zbDc8m3Va7giOcZxj",
        "MQ9ZsaRbIefYCjUFfbR5C3xqw4LCtjPOkA41YNl0GY",
		"XVEu2YrbbHoZjd2ZB3vfg", "tFwZ71DsltmUTh9taFhi0uCxfXHlySRDbTdmGTXPQ"))
    logger.info("Connected to twitter account")
except Exception:
    logger.error("Unable to open twitter account - credentials failed")
    sys.exit(-1)

mqttc = mosquitto.Mosquitto()
mqttc.on_message = on_message
mqttc.on_connect = on_connect

mqttc.connect(MQTT_SERVER, 1883, 60)

try:
    mqttc.loop_forever()
except KeyboardInterrupt:
    print 'Program closed via keyboard'
    logger.info('Program closed via keyboard')

mqttc.unsubscribe(TOPIC_STATUS)
mqttc.disconnect()

logger.info("Program closed")
sys.exit(0)
