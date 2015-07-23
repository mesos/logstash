#!/bin/bash

# kill all running instances of logstash
ps aux | grep /opt/logstash | grep -v grep | awk '{ print $2 }' | xargs kill &> /dev/null

# We first enabled it here to avoid the line above
# to fail the entire script.
set -e

/opt/logstash/bin/logstash -l /var/log/logstash.log -f /tmp/logstash/
