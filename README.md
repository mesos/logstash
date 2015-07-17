# Logstash Mesos Framework

A Mesos Framework for running Logstash in your cluster. You can configure logging for all your
other frameworks and have LogStash parse and send your logs to ElasticSearch.

## Overview

This framework will try to launch a logstash-process per slave.

The user writes logstash configuration files for the frameworks and docker images that he wants to support.
The logstash-executor will then be able to extract logs *out of* any docker container and parse
them according to the supplied configuration.

The configuration files can be supplied either trough the web UI of the scheduler or through writing
directly to the schedulers configuration directory.

## How Container Log Extraction is Implemented

For each log file within a docker container we run
```docker exec tail -f /my/logfile```
in the background. We then stream the contents into a local file within the logstash container.
This avoids doing intrusive changes (i.e, mounting a new ad-hoc volume) to the container.

The `tail -f` will steal some of the computing resources allocated to that container. But the
resource-restrictions imposed by Mesos will still be respected.


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

# Getting Started

*This project is under active development and instructions on how to actually run in a production
environment will be coming soon*.

This project has been sponsored by Cisco Cloud Services, as part of their effort to give back to the DevOps
community. Check out the MicroServices Infrastructure project for more.

You can find it [here](https://github.com/CiscoCloud/microservices-infrastructure).

# Requirements

The executor will require access to its docker host.

# Development

You can run the project directly on your machine or in Vagrant.
There is a `Vagrantfile` for the project if you want to run it there.

## Dependencies

- Java 7

## Building

You should copy the `local.properties.example` to `local.properties` and modify it with
your dockerhub username.

Also make sure you have the repos "logstash-scheduler" and "logstash-executor".

Then run:

```bash
$ ./gradlew :build
```

## Running the Tests

```bash
$ ./gradlew :system-test:build
```

## Starting a Local Cluster

3. Build

    ```bash
    $ cd /vagrant
    $ sudo ./gradlew build
    ```

## Sponsors
This project is sponsored by Cisco Cloud Services


# License

See `LICENSE` file.
