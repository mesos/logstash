# Logstash Mesos Framework

A [Mesos](http://mesos.apache.org/) framework for running [Logstash][logstash] in your cluster.
When you send a log event to a Mesos slave,
it will be parsed by Logstash and sent to central locations of your choice,
such as an ElasticSearch instance.

TODO why do we want a Logstash instance on every slave? Not clear.

This framework tries to launch a Logstash process on every Mesos slave.
Specifically, it accepts a Mesos offer if the offered slave does not yet have Logstash running,
and the offer has enough resources to run Logstash.
This does not guarantee the presence of Logstash on every slave,
but we have reason to believe that most clusters will gain high allocation (TODO why?).
This means this framework is suitable for non-critical logging of events which may be dropped,
such as resource usage statistics.
We currently advise using other systems for business-critical event logging,
such as PCI DSS events.


# Roadmap

## Version 1 - July 24th

- ☑ Automatic discovery of running frameworks, streaming logs from files inside the containers. (This feature has since been removed.)
- ☑ Shared Test- and Development- Setup with `mesos-elasticsearch`, `mesos-kibana`
- ☑ External LogStash Configuration (config files propagated from Master to Slaves)
- ☑ Support for outputting to Elastic Search
- ☑ Basic Error Handling
- ☑ Installation Documentation
- ☑ Design Documentation
- ☑ Configuration GUI
- ☑ REST API for managing Configurations

## Version 2 - Aug 14th

- ☑ Basic Failover Handling with Zookeeper
- ☑ Allow reconfiguring running frameworks
- ☑ Basic DCOS compliance (Alpha stage)

## Version 3 - ?

- ☐ DCOS certification
- ☐ Loss-less logging. Thoroughly ensure that no log messages are lost. (e.g. when a container has rolling log files)
- ☐ Enhanced Error and Failover Handling
- ☐ Support for streaming output from `docker log`
- ☐ Support for arbitrary Logstash Plugins
- ☐ Service Discovery (allow frameworks to discover the log service automatically, and configure themselves)


# Running

## Requirements

* A Mesos cluster at version 0.22.1 or above. We use features from 0.22:

  TODO why does our executor or scheduler Docker image have Mesos installed?
  We shouldn't actually need Mesos in the image; Mesos runs externally ...
  What does the `mesos` package provide?
  Does it provide the Mesos Java API library that we use?
  What is that library?
  org.apache.mesos
  What package provides this? How is it on the classpath?
  
      dependencies {
          compile "org.apache.mesos:mesos:${mesosVer}"
  
  What does this mean? What is `compile`?
  I think it means we use this: https://bintray.com/bintray/jcenter/org.apache.mesos%3Amesos/0.25.0/view#files/org/apache/mesos/mesos/0.25.0
  This includes the Mesos API jar
  TODO why do we install Mesos at version 0.25.0?

* That Mesos cluster must have the `docker` containerizer enabled.

* A Docker host must be running on every Mesos slave on port 2376.

* That Docker host must have an API version compatible with our Docker client,
  which is currently at version 1.20.

* Each Docker host must have access to the `mesos/logstash-executor` image.
  We maintain [releases of `mesos/logstash-executor` on Docker Hub][https://hub.docker.com/r/mesos/logstash-executor/].

* The `mesos/logstash-scheduler` image.


## Running directly on a native Mesos cluster

TODO

When running the scheduler directly (i.e from the command line or as a marathon app) you can use the corresponding java system properties.


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
      "image": "mesos/logstash-scheduler:0.0.6",
      "network": "HOST"
    }
  },
  "env": {
    "JAVA_OPTS": "-Dmesos.logstash.framework.name=logstash_framework -Dmesos.zk=zk://<zkserver:port>,<zkserver:port>/mesos"
  }
}
```
 
Please keep in mind that if you start the Logstash app as a Marathon app that this will start a 
scheduler on one arbitrary slave. The scheduler itself will try to start one (only one) executor 
on each node. To scale the application from within marathon makes no sense because only one scheduler
per framework is allowed to run and the framework scales itself to all slaves.  

The available DCOS configuration options are documented in [DCOS config.json](https://github.com/triforkse/universe/blob/version-1.x/repo/packages/L/logstash/0/config.json).
This shows how the DCOS parameters are translated to system properties.


## Running as DCOS app

To run the logstash framework as a DCOS app:

Add our logstash repository to DCOS:
```
dcos config append package.sources "https://github.com/triforkse/universe/archive/version-1.x.zip"
```

update DCOS:
```
dcos package update
```

and install the package
```
dcos package install --options=logstash-options.json logstash
```

the `logstash-options.json`-file in the above example is where you can configure
logstash with your own settings. An example can be found <a href="https://github.com/mesos/logstash/tree/master/dcos/logstash-options.json">here</a>.
See <a href="#fw_configuration">Framework options</a> for an explanation of the available configuration parameters.
 
Note: **Uninstalling the logstash DCOS package will shutdown the framework! See [Updating to new version](#newversion) how to preserve the your logstash slave and docker configuations.** 


## <a name="newversion"></a>Updating to a newer version (or reinstalling the app)

When reinstalling, you must manually go into your zookeeper ui and remove the path `/logstash/frameworkId`.
This is so that the reinstalled app will be able to register without losing the logstash docker and slave configurations.


## <a name="gui"></a> GUI

The GUI allows you to monitor the health of the cluster, see what is currently streaming and which
nodes have executors deployed.

The GUI is available whenever the scheduler is running. It can be accessed using HTTP through port `9092` on the
mesos slave where the scheduler is running.

### Dashboard

![Dashboard](docs/screenshot_dashboard.png)

The `Running Logstash Executors` shows the number of slaves where the framework (executors) is running on.
Usually that should match your number of slaves.

The `Observing Docker Containers` shows the number of docker containers logstash is actually observing log files from.
E.g. you've configured a docker configuration for two frameworks. And these framework are running let's say 4 and 5 docker containers 
somewhere in the cluster then 4+5=9 observing docker containers should be displayed.
 
### Nodes

![Nodes](docs/screenshot_nodes.png)

Here you see some detailed information about the slaves where the logstash framework is running on.
Slaves hostnames and some Mesos specfic information like TaskID and ExecutorID are shown.

Further you see all docker running containers discovered by the logstash framework and their image names.
That doesn't mean that logstash is currently observing log files from each docker container shown. 
Whether the logstash framework is observing log files from within these containers is indicated by a 
green bubble (see at the screenshot above). A gray bubble means that no files are monitored from within the container.

The status at the top right corner gives you a hint whether your logstash configuration could be successfully applied. 
If it's not `healthy` you should test your logstash configuration. 

Note: Currently there is no indication whether you monitoring file from the slave itself.     


# Configuration

The Logstash framework is configured at the time that the scheduler is started. Each configuration option can be passed in a large number of ways:

1. Command-line arguments, e.g. `java -jar logstash-mesos-scheduler.jar --logstash.heap-size=64`
2. Environment variables, e.g. `LOGSTASH_HEAP_SIZE=64 java -jar logstash-mesos-scheduler.jar`
3. A properties file, e.g. `echo 'logstash.heap-size=64' > ./application.properties && java -jar logstash-mesos-scheduler.jar` 
4. ...

| Command-line argument            | Environment variable           | Default    | What it does                                                                                                               |
| -------------------------------- | ------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------- |
| `--zk-url=U`                     | `ZK_URL=U`                     | Required   | The Logstash framework will find Mesos using ZooKeeper at URL `U`, which must be in the format `zk://host:port/zkNode,...` |
| `--zk-timeout=T`                 | `ZK_TIMEOUT=T`                 | `20000`    | The Logstash framework will wait `T` milliseconds for ZooKeeper to respond before assuming that the session has timed out  |
| `--framework-name=N`             | `FRAMEWORK_NAME=N`             | `logstash` | The Logstash framework will show up in the Mesos Web UI with name `N`, and the ZK state will be rooted at znode `N`        |
| `--webserver-port=P`             | `WEBSERVER_PORT=P`             | `9092`     | The scheduler will listen on TCP port `P` FIXME and host what on it? Not clear from code                                   |
| `--failover-timeout=T`           | `FAILOVER_TIMEOUT=T`           | `31449600` | Mesos will wait `T` seconds for the Logstash framework to failover before it kills all its tasks/executors                 |
| `--role=R`                       | `ROLE=R`                       | `*`        | The Logstash framework role will register with Mesos with framework role `U`.                                              |
| `--user=U`                       | `USER=U`                       | `root`     | Logstash tasks will be launched with Unix user `U`                                                                         |
| `--logstash.heap-size=N`         | `LOGSTASH_HEAP_SIZE=N`         | `32`       | The Logstash program will be started with `LS_HEAP_SIZE=N` FIXME what does this actually do                                |
| `--logstash.elasticsearch-url=U` | `LOGSTASH_ELASTICSEARCH_URL=U` | Absent     | If present, Logstash will forward its logs to an Elasticsearch instance at domain and port `U`                             |
| `--executor.cpus=C`              | `EXECUTOR_CPUS=C`              | `0.2`      | The Logstash framework will only accept resource offers with at least `C` CPUs. `C` must be a decimal greater than 0       |
| `--executor.heap-size=H`         | `EXECUTOR_HEAP_SIZE=H`         | `64`       | The memory allocation pool for the Logstash executor will be limited to `H` megabytes                                      |
| `--enable.failover=F`            | `ENABLE_FAILOVER=F`            | true       | If `F` is `"true"`, all executors and tasks will remain running after this scheduler exits FIXME what's the format for `F` |
| `--enable.collectd=C`            | `ENABLE_COLLECTD=C`            | false      | If `C` is `"true"`, Logstash will listen for collectd events on TCP/UDP port 5000 on all executors                         |
| `--enable.syslog=S`              | `ENABLE_SYSLOG=S`              | false      | If `S` is `"true"`, Logstash will listen for syslog events on TCP port 514 on all executors                                |


# REST API

Along with the GUI there is a RESTful API available. Currently is is only enabled if you also run the
GUI.

The available endpoints are:

```
GET /api/configs
```

Returns an array of configurations. (See format below)
The new framework will be available at `api/configs/{name}`.

```
POST /api/configs
```

Creates a new configuration for a framework. (See format below)

```
PUT /api/configs/{framework-name}
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
DELETE /api/configs/{framework-name}
```

Removes the configuration for this framework. Please make sure that framework-name is proper URL encoded (e.g. in JavaScript see `encodeURIComponent`)


# Building

To build mesos-logstash, first install these dependencies:

- Java Development Kit installed (Java 8)
- bash

Then run this command from this directory:

```
./gradlew --info clean compileJava
```

This will build the artifact: `./logstash-scheduler/build/libs/logstash-mesos-scheduler-X.Y.Z.jar`


## Run the Tests

To run the tests (including system tests) you will also need:

- A Docker daemon. This can be either

  - locally, or
  
  - remotely, e.g. using [Docker-Machine](https://docs.docker.com/machine/)) or boot2docker.

    If choosing this option, you need to do two things before running the tests:

    - tell your Docker client where your remote Docker daemon is.
      For example, if you are using `docker-machine`, and your machine is called `dev`,
      you can run in bash:
      
      ```
      eval $(docker-machine env dev)
      ```

    - Add an entry to the routing table
      to allow the scheduler, which runs outside of docker when testing, to
      communicate with the rest of the cluster inside docker.

      This command:
      
      ```
      sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
      ```
      
      sets up a route that allows packets to be routed from your scheduler (running locally on your
      computer) to any machine inside the subnet `172.17.0.0/16`, using your docker host as gateway.

Then, to run the all tests:

```
./gradlew -a --info build
```

# Limitations

Log files will be streamed into local files within the logstash-mesos container.
This requires disk space
which is hard to estimate beforehand, since it depends on the number of available log files.

The intention is to do a best guess when allocating resources from Mesos (Work in Progress).

## Security

There is no mechanism which ensures that the logstash output might overlap with other
logstash configurations. In other words: logstash might observe one framework
and output to the same destination it's using for another framework. 

# Sponsors

This project is sponsored by `Cisco Cloud Services`. Thank you for contributing to the Open Source
community!


[logstash]: https://www.elastic.co/products/logstash