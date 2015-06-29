#!/bin/sh

PROCESS_NAME="/usr/bin/python /home/pi/uqferry/ferry.py"

if [ -z `pgrep -f -x "$PROCESS_NAME"` ]
then
    echo "Restarting $PROCESS_NAME."
    cmd="$PROCESS_NAME &"
    eval $cmd
fi
