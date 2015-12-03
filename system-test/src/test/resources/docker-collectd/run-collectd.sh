#!/bin/sh

echo "${COLLECTD_CONF}" > /tmp/collectd.conf
collectd -f -C /tmp/collectd.conf
