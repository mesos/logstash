FROM containersol/mesos:0.25.0-0.2.70.ubuntu1404

RUN apt-get update && apt-get install -y software-properties-common

ADD ./build/docker/logstash-mesos-scheduler.jar /tmp/logstash-mesos-scheduler.jar
ADD start-scheduler.sh /tmp/start-scheduler.sh

ENTRYPOINT ["/tmp/start-scheduler.sh"]