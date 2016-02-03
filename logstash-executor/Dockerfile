FROM mesos/logstash-commons:latest

RUN echo 'deb http://packages.elastic.co/logstash/2.1/debian stable main' > /etc/apt/sources.list.d/logstash.list && \
  apt-get update && \
  apt-get install -y --force-yes logstash && \
  rm -rf /var/lib/apt/lists/*

RUN bash -c "/opt/logstash/bin/plugin install logstash-output-syslog"
RUN bash -c "/opt/logstash/bin/plugin install logstash-output-kafka"

ADD ./build/docker/logstash-mesos-executor.jar /tmp/logstash-mesos-executor.jar

ADD ./start-executor.sh /tmp/start-executor.sh

# See https://github.com/elastic/logstash/issues/3127
RUN ln -s /lib/x86_64-linux-gnu/libcrypt.so.1 /usr/lib/x86_64-linux-gnu/libcrypt.so

ENTRYPOINT ["/tmp/start-executor.sh"]
