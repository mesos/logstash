FROM ubuntu:14.04
MAINTAINER Mesosphere <support@mesosphere.io>

RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
RUN apt-get -y update
RUN apt-get -y install mesos=0.22.1-1.0.ubuntu1404

RUN apt-get update && apt-get install -y software-properties-common

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

ADD ./build/docker/logstash-mesos-scheduler.jar /tmp/logstash-mesos-scheduler.jar
ADD start-scheduler.sh /tmp/start-scheduler.sh

ENTRYPOINT ["/tmp/start-scheduler.sh"]