# Logstash
*Coming soon!* Logstash on Mesos

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

This framework requires:
* a running [Mesos](http://mesos.apache.org) cluster.

The framework can be run by building the code, the docker images, transferring the code to the Mesos cluster and
launching the framework _scheduler_.

# How to build
```
$ ./gradlew build
```

Alteratively:
* Use [gdub](https://github.com/dougborg/gdub) which runs the gradle wrapper from any subdirectory, so that you don't need to deal with relative paths
* Use [Vagrant](#building-with-vagrant)

```bash
$ java -jar logstash-scheduler.jar -m MASTER_IP:5050 -f /path/to/logstashconfig
```

## Alternative ways of building
### Building with Vagrant

Prerequisites:
* Running Docker service
* Vagrant 1.7.2 and VirtualBox 4.3.28 (at least the versions have been tested)

**Note:** Currently you need to sudo the build command or the Docker part will fail. This will be fixed shortly.

Actions to perform to start in Mac:

1. Start Vagrant from project directory:

    ```bash
    $ vagrant up
    ```

2. When completed SSH into the VM:

    ```bash
    $ vagrant ssh
    ```

3. Build

    ```bash
    $ cd /vagrant
    $ sudo ./gradlew build
    ```

## Sponsors
This project is sponsored by Cisco Cloud Services

## License
Apache License 2.0
