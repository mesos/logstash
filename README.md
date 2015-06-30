# Logstash Mesos Framework

A Mesos Framework for running Logstash in your cluster. You can configure logging for all your
other frameworks and have LogStash parse and send your logs to ElasticSearch.


# Roadmap

## Version 1 - July 31st

- ☑ Automatic discovery of running frameworks, streaming logs from containers.
- ☐ Shared Test- and Development- Setup with `mesos-elasticsearch`, `mesos-kibana`
- ☐ External LogStash Configuration (config files propagated from Master to Slaves)
- ☐ Support for outputting to Elastic Search
- ☐ Installation Documentation
- ☐ Design Documentation

## Version 2 - ?

- ☐ Loss-less logging. Thoroughly ensure that no log messages are lost. (e.g. when a container has rolling log files)
- ☐ DCOS certification

## Version 3 - ?

- ☐ Service Discovery (allow other frameworks to discover the log service automatically, and configure themselves)

- ☐ Configuration GUI

# Getting Started

*This project is under active development and instructions on how to actually run in a production
environment will be coming soon*.

This project has been sponsored by Cisco Cloud Services, as part of their effort to give back to the DevOps
community. Check out the MicroServices Infrastructure project for more.

You can find it [here](https://github.com/CiscoCloud/microservices-infrastructure).

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
