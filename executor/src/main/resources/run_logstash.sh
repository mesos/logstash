#!/bin/bash

CONFIG_LOCATION=/tmp/logstash/
LOGSTASH=/opt/logstash/bin/logstash

# kill all running instances of logstash
ps aux | grep /opt/logstash | grep -v grep | awk '{ print $2 }' | xargs kill &> /dev/null

$LOGSTASH -l /var/log/logstash.log -f $CONFIG_LOCATION