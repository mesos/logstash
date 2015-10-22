#!/bin/bash

java $JAVA_OPTS -Djava.library.path=/usr/local/lib -jar ./logstash-mesos-executor.jar $@