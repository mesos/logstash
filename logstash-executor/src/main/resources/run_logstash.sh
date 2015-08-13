#!/bin/bash

# Failsafe to ensure that we have no previous logstash instances running.
# This SHOULD never happen, since the executor stops the previous process
# before calling this script.

ps aux | grep /opt/logstash | grep -v grep | awk '{ print $2 }' | xargs kill &> /dev/null

# We first enabled it here to avoid the line above
# to fail the entire script.

set -e

# Run

HOME=/root /opt/logstash/bin/logstash -l /var/log/logstash.log -f /tmp/logstash/
