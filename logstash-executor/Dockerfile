FROM ubuntu:14.04

RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list && \
  apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
  apt-get -y update && \
  apt-get -y install mesos=0.22.1-1.0.ubuntu1404 && \
  rm -rf /var/lib/apt/lists/*

RUN echo 'deb http://packages.elasticsearch.org/logstash/1.5/debian stable main' | tee /etc/apt/sources.list.d/logstash.list

RUN apt-get update && apt-get install -y software-properties-common

RUN apt-get install -y --force-yes logstash

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

RUN bash -c "/opt/logstash/bin/plugin install logstash-output-syslog"
RUN bash -c "/opt/logstash/bin/plugin install logstash-output-kafka"

ADD ./build/docker/logstash-mesos-executor.jar /tmp/logstash-mesos-executor.jar

ADD ./build/resources/main/run_logstash.sh /tmp/run_logstash.sh
ADD ./start-executor.sh /tmp/start-executor.sh

# See https://github.com/elastic/logstash/issues/3127
RUN ln -s /lib/x86_64-linux-gnu/libcrypt.so.1 /usr/lib/x86_64-linux-gnu/libcrypt.so

ENTRYPOINT ["/tmp/start-executor.sh"]
