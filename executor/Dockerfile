FROM mesosphere/mesos:0.22.1-1.0.ubuntu1404


RUN echo 'deb http://packages.elasticsearch.org/logstash/1.5/debian stable main' | tee /etc/apt/sources.list.d/logstash.list
RUN apt-get update -y
RUN apt-get install -y --force-yes logstash


ADD ./build/docker/logstash-executor-dependencies.jar /tmp/logstash-executor-dependencies.jar

ADD ./build/docker/logstash-executor.jar /tmp/logstash-executor.jar
ADD ./build/resources/main/run_logstash.sh /tmp/run_logstash.sh



ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib","-jar", "/tmp/logstash-executor.jar"]
