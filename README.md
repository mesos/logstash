# Logstash Mesos Framework

A [Mesos](http://mesos.apache.org/) framework for running logstash in your cluster. You can configure logging for all your
other frameworks and have logstash parse and send your logs to ElasticSearch.

## Overview

This framework will try to launch a logstash-process per slave.

The user writes logstash configuration files for the frameworks and docker images that he wants to support.
The logstash-executor will then be able to extract logs *out of* any docker container and parse
them according to the supplied configuration.

The configuration files can be supplied either trough the web UI of the scheduler or through writing
directly to the schedulers configuration directory.


# Roadmap

## Version 1 - July 31st

- ☑ Automatic discovery of running frameworks, streaming logs from containers.
- ☑ Shared Test- and Development- Setup with `mesos-elasticsearch`, `mesos-kibana`
- ☑ External LogStash Configuration (config files propagated from Master to Slaves)
- ☑ Support for outputting to Elastic Search
- ☐ Basic Error Handling
- ☐ Installation Documentation
- ☐ Design Documentation
- ☐ Basic DCOS compliance (Alpha stage)

## Version 2 - ?

- ☐ Loss-less logging. Thoroughly ensure that no log messages are lost. (e.g. when a container has rolling log files)
- ☐ DCOS certification
- ☐ Enhanced Error Handling
- ☐ Allow reconfiguring running frameworks

## Version 3 - ?

- ☐ Service Discovery (allow other frameworks to discover the log service automatically, and configure themselves)

- ☐ Configuration GUI

# How to Run the Framework

[TODO] 

## Requirements

Mesos 0.22.1 (or compatible).

The executor will require access to its docker host in order to be able to discover and stream from docker containers.

Since the executor runs inside its own docker container it will try to reach its host using:
`http://slavehostname:2376`.

In order to stream the content of monitored log files each docker container hosting these files must have the following 
binaries installed and executable:

- tail

- sh


## Configuration

### Format
### Matching

## Run on Mesos

[TODO]

## Run on Marathon

[TODO]

## UI

## Use Case NGINX

# Technical Details

The mesos-logstash framework is written in Java (Version 8).

## How we extract the logs

Currently we only support monitoring log files within a running docker container. See configuration section how to
specify the log file location. 

For each log file within a docker container we run
```docker exec <observed-container> tail -f /my/configured/logfile```
in the background. We then stream the contents into a local file within the logstash container.
This avoids doing intrusive changes (i.e, mounting a new ad-hoc volume) to the container.

The file size of each streamed log file withing the logstash container is limited (currently max. 5MB). When the file size 
exceeds that limit the file content is truncated. This might cause loss of data but is in our opinion still acceptable (best effort).  


The `tail -f` will steal some of the computing resources allocated to that container. But the
resource-restrictions imposed by Mesos will still be respected. 

## Run the Tests

To run the tests locally you need to fulfill the following requirements:

- Java Development Kit installed (Java 8)
- Docker daemon running (either locally or using e.g. [Docker-Machine](https://docs.docker.com/machine/))

If your Docker daemon is not running natively on your machine (e.g. on a Mac or if you're using docker-machine) you have
to export the DOCKER_* variables (e.g. for docker-machine use `eval $(docker-machine env dev)`).

Run `gradle test` to run the all tests.  

### Routes
When testing against non-local docker host (e.g docker-machine, boot2docker) you will need to add a route
to get the tests to run.

The reason is to allow the scheduler, which runs outside of docker when testing, to
communicate with the rest of the cluster inside docker.

This command:
```
sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
```
Sets up a route that allows packets to be routed from your scheduler (running locally on your
computer) to any machine inside the subnet `172.17.0.0/16`, using your docker host as gateway.

# Limitations
Log files will be streamed into local files within the logstash-mesos container. This requires disk space
which is hard to estimate beforehand, since it depends on the number of available log files.

The intention is to do a best guess when allocating resources from Mesos (Work in Progress).

# Missing Features
These features are yet to be implemented:
- Processing non-dockerized log files (meaning, log files available directly on the slaves).
- Logging cannot be reconfigured once logstash-mesos has started streaming from a container

# Security
*To be Written*


## Sponsors
This project is sponsored by Cisco Cloud Services


# License

See `LICENSE` file.
