FROM mesosphere/mesos:0.22.1-1.0.ubuntu1404

ADD ./build/docker/logstash-scheduler-dependencies.jar /tmp/logstash-scheduler-dependencies.jar

ADD ./conf.conf /tmp/conf.conf

ADD ./build/docker/logstash-scheduler.jar /tmp/logstash-scheduler.jar

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib", "-jar", "/tmp/logstash-scheduler.jar" ]
