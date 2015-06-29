#!/usr/local/bin/python2.7

import sys
import json
import mosquitto
from time import gmtime, strftime
from math import sqrt, sin, cos, asin, radians
import datetime
import logging
import logging.handlers

stateNowhere = 0
stateApproaching = 1
stateDocked = 2
stateDeparting = 3

status_string = {0: 'nowhere nearby', 1: 'approaching', 2: 'docked', 3: 'departed'}
current_state = {}

TOPIC_JSON = "/uq/ferry/JSON"
TOPIC_STATUS = "/uq/ferry/status"
LOG_FILENAME = '/tmp/ferry_status.log'
MQTT_SERVER = "winter.ceit.uq.edu.au"

COORDS_UQ_TERMINAL = (-27.49668, 153.01953333)
DISTANCE_APPROACH = 200.0 # metres??
DISTANCE_DOCKED = 25.0 # metres

radius = 0.0

ferry_names = {
"503576100": "Baneraba",
"503575500": "Barrambin",
"503576200": "Beenung-urrung",
"503575700": "Binkinba",
"503016000": "Bulimba",
"503576900": "Gootcha",
"503576800": "Kuluwin",
"503575300": "Kurilpa",
"503305400": "Lucinda",
"503576700": "Mahreel",
"503576400": "Meeandah",
"503575800": "Mianjin",
"503575600": "Mirbarpa",
"503575900": "Mooroolbin",
"503576100": "Baneraba",
"503577200": "Mudherri",
"503586200": "Spirit of Brisbane",
"503575400": "Tugulawa",
"503576300": "Tunamun",
"503577100": "Walan",
"503576500": "Wilwinpa",
"503576600": "Ya-wa-gara"
}

logger = logging.getLogger("ferry")
logger.setLevel(logging.INFO)

# create file handler
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000*1024, backupCount=5)

#craete a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handler to the logger
logger.addHandler(handler)

def get_ferry_name(vessel_id):
    if not vessel_id in ferry_names:
        return ferry_names[vessel_id]
    else:
        logger.info("No vessel name: %s", vessel_id)
        s = 'vessel ID %s has no name' % (vessel_id)
        mqttc.publish("/uq/ferry/log", payload=s, qos=0, retain=True)
        return "New Ferry"

def earthRadius(latitude):
    ''' Calculate the radius of the earth at specified latitude (the world may not be flat,
    but neith is it round). Requires latitude in degrees, returns radius in Km. '''

    lat = radians(latitude)
    a = 6378.1370 # Earth Equatorial Radius
    b = 6356.7523 # Earth Polar Radius

    top = (a**2 * cos(lat))** 2 + (b**2 * sin(lat))**2
    bottom = (a * cos(lat))** 2 + (b * sin(lat))** 2

    return sqrt (top / bottom)

def distance2(coord1, coord2):
    ''' Calculate the distance in Km between two points on the earth, given their (latitude
    and longitude in degrees). Calculation taken from Haversine Formula (from R.W. Sinnott,
    "Virtues of the Haversine", Sky and Telescope, vol. 68, no. 2, 1984, p. 159)'''

    dlon = radians(coord1[1] - coord2[1])
    dlat = radians(coord1[0] - coord2[0])
    a = sin(dlat/2)** 2 + cos(radians(coord2[0])) * cos(radians(coord1[0])) * sin(dlon/2)** 2
    c = 2 * asin( min(1, sqrt(a)) )
    d = radius * c # in Km
    return d

def describe_ferry_status(vessel_id, heading, speed, dist):
    """Describe the status of any and all ferries with respect to a ferry terminal. All
    this state machine does is ensure that we get just one indication of ferry status,
    and not one every 6 seconds."""

    vessel_name = get_ferry_name(vessel_id)
    state_current = stateNowhere if not vessel_id in current_state else current_state[vessel_id]
    # Handle the case of a first sighting, and set the current_state
    if not vessel_id in current_state:
        if dist < DISTANCE_APPROACH:
            if dist <= DISTANCE_DOCKED and speed < 1.0:
                current_state[vessel_id] = stateDocked
                state_current = stateDocked
            elif dist < DISTANCE_APPROACH:
                current_state[vessel_id] = stateApproaching
                state_current = stateApproaching
            else:
                current_state[vessel_id] = stateDeparting
                state_current = stateDeparting
        else:
            current_state[vessel_id] = stateNowhere
    # Handle the case of a ferry that was a long way off
    elif current_state[vessel_id] == stateNowhere:
        if dist <= DISTANCE_DOCKED and speed < 1.0:
            current_state[vessel_id] = stateDocked
        elif dist < DISTANCE_APPROACH:
            if dist < DISTANCE_APPROACH:
                current_state[vessel_id] = stateApproaching
            else:
                current_state[vessel_id] = stateDeparting
    # Handle case for a ferry that was approaching the terminal
    elif current_state[vessel_id] == stateApproaching:
        if dist < DISTANCE_DOCKED and speed < 1.0:
            current_state[vessel_id] = stateDocked
        elif dist > DISTANCE_APPROACH:
            current_state[vessel_id] = stateDeparting
    # Handle the case of a ferry that was docked
    elif current_state[vessel_id] == stateDocked:
        if speed > 1.0:
            current_state[vessel_id] = stateDeparting
    # Finally, handle the case of a ferry that was departing
    elif current_state[vessel_id] == stateDeparting:
        if dist > DISTANCE_APPROACH:
            current_state[vessel_id] = stateNowhere
    #If we get here, then we have really suffed up our logic
    else:
        current_state[vessel_id] = stateNowhere
    	logger.error("NEXT_STATE: > Stuff UP!  'describe_ferry_status' in a wierd state")

    next_state = current_state[vessel_id]
    next_state_string = status_string[next_state]
    logger.debug("NEXT STATE: %s moved to state %s, speed: %d distance %d", vessel_name, next_state_string,  speed, dist)

    if next_state != state_current and next_state in {stateApproaching, stateDocked, stateDeparting}:
        now = datetime.datetime.now()
        time_display = now.strftime("%I:%M %p")
        json_str = json.dumps({ "vesselID": vessel_id, "vesselName": vessel_name, \
            "status": next_state_string, "distance": dist, "time": time_display})
        mqttc.publish(TOPIC_STATUS, json_str)

def on_connect(mosq, obj, msg):
    # Subscribe to the topic each time we connect - for safety.
    mqttc.subscribe(TOPIC_JSON, 0)
    logger.info("Successfully (re)subscribed to topic '%s'", TOPIC_JSON)

def on_message(mosq, obj, msg):
    """ Each incoming MQTT message contains JSON encoded NEMa data from the AIS.
    Let's extract the latitude and longitude and work out if the ferry is between
    St Lucia CityCat terminal and The end of Sir William MacGreggor Driv.  If it is
    then we can work out the status of the freey from its direction and speed."""

    data = json.loads(msg.payload)
    current_coords = (data["latitude"], data["longitude"])
    r = distance2(current_coords, COORDS_UQ_TERMINAL)
    logger.debug("%s located %s is %d from the Ferry Terminal (in metres)", get_ferry_name(data["mssi"]), current_coords, 1000*r)
    describe_ferry_status(data["mssi"], data["course"], data["speed"], r*1000)

#
# Main starts here.
#

mqttc = mosquitto.Mosquitto()
mqttc.on_message = on_message
mqttc.on_connect = on_connect

radius = earthRadius(COORDS_UQ_TERMINAL[0])

mqttc.connect(MQTT_SERVER, 1883, 60)

try:
    mqttc.loop_forever()
except KeyboardInterrupt:
    print 'Program closed via keyboard'
