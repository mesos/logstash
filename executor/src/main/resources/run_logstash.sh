#!/bin/bash

SCRIPT_LOCATION=/tmp/logstash.conf
TEMP_SCRIPT_LOCATION=/tmp/logstash.temp.conf

cp $SCRIPT_LOCATION $TEMP_SCRIPT_LOCATION
/opt/logstash/bin/logstash -f $TEMP_SCRIPT_LOCATION -e 'input { file { path => "/dev/null" } }' &

LOGSTASH_PID=$!
echo "PID $LOGSTASH_PID"

function stop() {
  echo "Killing logstash $LOGSTASH_PID"
  kill $LOGSTASH_PID
  exit
}

trap stop SIGINT

while :
do
  touch $TEMP_SCRIPT_LOCATION
  diff $SCRIPT_LOCATION $TEMP_SCRIPT_LOCATION > /dev/null
  if [ $? -ne 0 ]; then
    echo "Config changed. Redeploying.."
    cp $SCRIPT_LOCATION $TEMP_SCRIPT_LOCATION

    echo "Killing logstash $LOGSTASH_PID"
    kill $LOGSTASH_PID

    echo "Restarting.."
    /opt/logstash/bin/logstash -f $TEMP_SCRIPT_LOCATION  -e 'input { file { path => "/dev/null" } }' &
    LOGSTASH_PID=$!
    echo "PID $LOGSTASH_PID"
  fi
  sleep 1
done
