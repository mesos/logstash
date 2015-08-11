# Logstash Mesos Framework

A [Mesos](http://mesos.apache.org/) framework for running logstash in your cluster.
You can configure logging for all your other frameworks and have logstash parse and send
your logs to ElasticSearch.

## Overview

This framework will try to launch a logstash-process per slave.

The user writes logstash configuration files for the frameworks and docker images that he wants to support.
Logstash-mesos will then be able to extract logs *out of* any docker container and parse
them according to the supplied configuration.

The configuration files can be supplied either trough the web UI of the scheduler or through writing
directly to the schedulers configuration directory.

# Roadmap

## Version 1 - July 24st

- ☑ Automatic discovery of running frameworks, streaming logs from containers.
- ☑ Shared Test- and Development- Setup with `mesos-elasticsearch`, `mesos-kibana`
- ☑ External LogStash Configuration (config files propagated from Master to Slaves)
- ☑ Support for outputting to Elastic Search
- ☑ Basic Error Handling
- ☑ Installation Documentation
- ☑ Design Documentation
- ☑ Configuration GUI
- ☑ REST API for managing Configurations

## Version 2 - Aug 7th

- ☑ Basic Failover Handling with Zookeeper
- ☑ Allow reconfiguring running frameworks
- ☐ Basic DCOS compliance (Alpha stage)

## Version 3 - ?

- ☐ DCOS certification
- ☐ Loss-less logging. Thoroughly ensure that no log messages are lost. (e.g. when a container has rolling log files)
- ☐ Enhanced Error and Failover Handling
- ☐ Support for Logstash Plugins
- ☐ Service Discovery (allow frameworks to discover the log service automatically, and configure themselves)


## Requirements

Mesos 0.22.1 (or compatible).

### Access to Docker Host

The executor will require access to its docker host in order to be able to discover
and stream from docker containers.

Since the executor runs inside its own docker container it will try to reach its host using:
`http://<slavehostname>:2376`.

### Requirements on Docker Containers

In order to stream the content of monitored log files, each docker container hosting these files 
must have the following binaries installed and executable:

- tail
- sh

## Running as Marathon app

To run the logstash framework as a Marathon app use the following app template (save e.g. as logstash.json):
Update the JAVA_OPTS attribute with your Zookeeper servers.

 ```
 {
   "id": "/logstash",
   "cpus": 1,
   "mem": 1024.0,
   "instances": 1,
   "container": {
     "type": "DOCKER",
     "docker": {
       "image": "mesos/logstash-scheduler:0.0.2",
       "network": "HOST"
     }
   },
   "env": {
     "JAVA_OPTS": "-Dmesos.logstash.framework.name=logstash_framework -Dmesos.zk=zk://<zkserver:port>,<zkserver:port>/mesos"
   }
 }

 ```
 

## <a name="configuration"></a> Configuration
[TODO]

## GUI

The scheduler will by default start with a GUI enabled. You can disable this by setting the system
property `mesos.logstash.noUI=true`.

The GUI allows you to monitor the health of the cluster, see what is currently streaming and which
nodes have executors deployed.

## REST API

Along with the GUI there is a RESTful API available. Currently is is only enabled if you also run the
GUI.

The available endpoints are:

```
GET api/configs
```

Returns an array of configurations. (See format below)
The new framework will be available at `api/configs/{name}`.

```
POST /configs
```

Creates a new configuration for a framework. (See format below)

```
PUT /configs/{framework-name}
```

Updates an existing framework config. Please make sure that framework-name is proper URL encoded (e.g. in JavaScript see `encodeURIComponent`)

// TODO: Write about native config endpoints.

__Expected Format__

```js
{
    "name": "String", // The name of the docker image to match when,
    "input": "String" // The Logstash configuration segment for this framework.
}
```

```
DELETE /configs/{framework-name}
```

Removes the configuration for this framework. Please make sure that framework-name is proper URL encoded (e.g. in JavaScript see `encodeURIComponent`)

# Technical Details

The mesos-logstash framework is written in Java (Version 8).

## How we extract the logs

Currently we only support monitoring log files within a running docker container. See configuration section how to
specify the log file location. 

For each log file within a docker container we run
```docker exec <observed-container> tail -f /my/configured/logfile```
in the background. We then stream the contents into a local file within the logstash container.
This avoids doing intrusive changes (i.e, mounting a new ad-hoc volume) to the container.

The file size of each streamed log file within the logstash container is limited
(currently max. 5MB). When the file size
exceeds that limit the file content is truncated.
This might cause loss of data but is in our opinion still acceptable (best effort).  


The `tail -f` will steal some of the computing resources allocated to that container. But the
resource-restrictions imposed by Mesos will still be respected. 

## Run the Tests

To run the tests locally you need to fulfill the following requirements:

- Java Development Kit installed (Java 8)
- Docker daemon running (either locally or using e.g. [Docker-Machine](https://docs.docker.com/machine/))

If your Docker daemon is not running natively on your machine
(e.g. on a Mac or if you're using docker-machine) you have
to export the DOCKER_* variables (e.g. for docker-machine use `eval $(docker-machine env dev)`).

Run `gradle test` to run the all tests.  

### Routes

When testing against non-local docker host (e.g docker-machine, boot2docker)
you will need to add a route to get the tests to run.

The reason is to allow the scheduler, which runs outside of docker when testing, to
communicate with the rest of the cluster inside docker.

This command:
```
sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
```
Sets up a route that allows packets to be routed from your scheduler (running locally on your
computer) to any machine inside the subnet `172.17.0.0/16`, using your docker host as gateway.

# Limitations

Log files will be streamed into local files within the logstash-mesos container.
This requires disk space
which is hard to estimate beforehand, since it depends on the number of available log files.

The intention is to do a best guess when allocating resources from Mesos (Work in Progress).

# Security

The framework will process log files of any docker container which is running on the same slave
node and which are accessable via `docker exec <observed-container> tail -f /my/configured/logfile`. 

There is no mechanism which ensures that you're authorized to monitor the log files of
a specific framework running on the same cluster/node.

There is no mechanism which ensures that the logstash output might overlap with other
logstash configurations. In other words: logstash might observe one framework
and output to the same destination it's using for another framework. 

# Sponsors

This project is sponsored by `Cisco Cloud Services`. Thank you for contributing to the Open Source
community!

