FROM ubuntu:15.10

RUN apt-get update && apt-get -y install collectd && rm -rf /var/lib/apt/lists/*

ADD run-collectd.sh /tmp/run-collectd.sh
RUN chmod +x /tmp/run-collectd.sh

ENTRYPOINT /tmp/run-collectd.sh
