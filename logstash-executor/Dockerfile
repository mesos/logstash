FROM mesos/logstash-commons:latest

RUN echo 'deb http://packages.elasticsearch.org/logstash/1.5/debian stable main' | tee /etc/apt/sources.list.d/logstash.list

RUN apt-get update

RUN apt-get install -y --force-yes logstash

RUN bash -c "/opt/logstash/bin/plugin install logstash-output-syslog"
RUN bash -c "/opt/logstash/bin/plugin install logstash-output-kafka"

ADD ./build/docker/logstash-mesos-executor.jar /tmp/logstash-mesos-executor.jar

ADD ./build/resources/main/run_logstash.sh /tmp/run_logstash.sh
ADD ./start-executor.sh /tmp/start-executor.sh

# See https://github.com/elastic/logstash/issues/3127
RUN ln -s /lib/x86_64-linux-gnu/libcrypt.so.1 /usr/lib/x86_64-linux-gnu/libcrypt.so

ENTRYPOINT ["/tmp/start-executor.sh"]
