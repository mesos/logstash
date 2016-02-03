FROM mesos/logstash-commons:latest

ADD ./build/docker/logstash-mesos-scheduler.jar /tmp/logstash-mesos-scheduler.jar

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib", "-jar", "/tmp/logstash-mesos-scheduler.jar"]