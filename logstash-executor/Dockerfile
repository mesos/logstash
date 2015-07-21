FROM mesosphere/mesos:0.22.1-1.0.ubuntu1404

RUN echo 'deb http://packages.elasticsearch.org/logstash/1.5/debian stable main' | tee /etc/apt/sources.list.d/logstash.list

RUN apt-get update && apt-get install -y software-properties-common

RUN apt-get install -y --force-yes logstash

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer && \
  rm -rf /var/lib/apt/lists/* && \
  rm -rf /var/cache/oracle-jdk8-installer

ADD ./build/libs/logstash-executor-deps.jar /tmp/logstash-executor-deps.jar
ADD ./build/libs/logstash-executor.jar /tmp/logstash-executor.jar

ADD ./build/resources/main/run_logstash.sh /tmp/run_logstash.sh

# See https://github.com/elastic/logstash/issues/3127
RUN ln -s /lib/x86_64-linux-gnu/libcrypt.so.1 /usr/lib/x86_64-linux-gnu/libcrypt.so

ENTRYPOINT ["java", "-Djava.library.path=/usr/local/lib","-jar", "/tmp/logstash-executor.jar"]
