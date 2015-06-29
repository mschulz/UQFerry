#!/usr/bin/python

import serial
import time
import json
import mosquitto
import sys
import logging
import logging.handlers

# set up logging
LOG_FILENAME='/tmp/ferry.log'

logger = logging.getLogger("ferry")
logger.setLevel(logging.INFO)

# create file handler
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1000*1024, backupCount=5)

#craete a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handler to the logger
logger.addHandler(handler)

def dec2bin(string_num):
    num = int(string_num)
    mid = []
    i = 0
    while True:
        if i == 6: break
        num,rem = divmod(num, 2)
        mid.append(rem)
        i = i+1

    return ''.join([str(x) for x in mid[::-1]])


def speed(string_num):
    dec = int(string_num,2)
    speed = float(dec) / 10
    return speed

def lon(string_num):
    dec = int(string_num,2)
    if dec <= 134217728:
        lon = float(dec) / 600000
    else:
        lon = float(268435456 - dec) / 600000 * (-1)
    return lon

def lat(string_num):
    dec = int(string_num,2)
    if dec <= 67108864:
        lat = float(dec) / 600000
    else:
        lat = float(134217728 - dec) / 600000 * (-1)
    return lat

def course(string_num):
    dec = int(string_num,2)
    course = float(dec) / 10
    return course

def on_connect(mosq, obj, rc):
    if rc == 0:
        logger.info("mosquitto: Connected successfully")
        return
# Handle the error conditions
    if rc == 1:
        logger.error("mosquitto: unacceptable protocol version\n")
    elif rc == 2:
        logger.error("mosquitto: identifier rejected")
    elif rc == 3:
        logger.error("mosquitto: server winter.ceit.uq.edu.au unavailable")
    elif rc == 4:
        logger.error("mosquitto: bad user name or password")
    else:
        logger.error("mosquitto: not authorized")
# Go no further with this program
    sys.exit(1)

def process_line(line):
#    print line
    local = time.strftime('%H:%M:%S +10',time.localtime(time.time()))
    date = time.strftime('%B %d,%Y',time.localtime(time.time()))
    nmea = line.split(',')[5]
    msg = ''
    for i in range(len(nmea)):
        sixbit = ord(nmea[i]) - 48
        if sixbit > 40:
            sixbit -= 8
        temp = dec2bin(sixbit)
        msg += temp
    msg_type = msg[0:6]
    msg_type = int(msg_type,2)
    logger.debug('Message Type: %d', msg_type)
    if msg_type == 18:
        logger.debug('SUCCESS!')
        msg_mssi = int(msg[8:38], 2)
        msg_accuracy = int(msg[56:57], 2)
        msg_heading = int(msg[124:133], 2)
        msg_timestamp = int(msg[133:139], 2)
        msg_speed = speed(msg[46:56])
        msg_lon = lon(msg[57:85])
        msg_lat = lat(msg[85:112])
        msg_course = course(msg[112:124])
        return json.dumps({ 'mssi': str(msg_mssi), \
                'speed': msg_speed, \
                'longitude': msg_lon, \
                'latitude': msg_lat, \
                'course': msg_course, \
                'true heading': msg_heading, \
                'position accuracy': msg_accuracy, \
                'time stamp': msg_timestamp, \
                'type': msg_type, \
                'time': local, \
                'date': date })
    else:
        return ""

def open_AIS_connection():
# First, try all known USB serial ports until we have success, else quit.
    try:
        ser=serial.Serial(port='/dev/ttyACM0', baudrate=38400,timeout=10)
        logger.info("Open serial port '/dev/ttyACM0'")
    except:
        try:
            ser=serial.Serial(port='/dev/ttyUSB0', baudrate=38400,timeout=10)
            logger.info("Open serial port '/dev/ttyUSB0'")
        except:
            logger.error("Cannot connect to serial port '/dev/tty{ACM0,USB0}'")
            sys.exit(1)
    return ser

def on_publish(mosq, obj, mid):
    logger.debug("Message %s published.", str(mid))

def main():
    ser = open_AIS_connection()
    ser.close()
    ser.open()

    rc = 0
    mqttc = mosquitto.Mosquitto()

    mqttc.on_connect = on_connect
#    mqttc.on_publish = on_publish

    try:
        mqttc.connect("winter.ceit.uq.edu.au", 1883, 60)
    except:
        logger.error('Cannot connect to MQTT server')
        sys.exit(1)

    while mqttc.loop() == 0:
        try:
            line = ser.readline()
            logger.debug('Raw AIS: %s', line)
            if line.startswith('!AIVDM,'):
                logger.debug('Useful line: %s', line)
                s = process_line(line)
                if len(s) > 0:
                    logger.debug('Processed line: %s', s)
                    mqttc.publish("/uq/ferry/raw", line, 0)
                    mqttc.publish("/uq/ferry/JSON", s, 0)
        except KeyboardInterrupt:
            logger.info('Program closed via keyboard')
            sys.exit(0)
        except:
            pass
    ser.close()

main()
