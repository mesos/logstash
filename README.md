# Logstash Mesos Framework

This is a [Mesos](http://mesos.apache.org/) framework for running [Logstash][logstash] in your cluster.
When another program on any Mesos slave logs an event,
it will be parsed by Logstash and sent to central log locations.
There are many ways to log an event, such as [syslog](https://en.wikipedia.org/wiki/Syslog) or writing to a log file.
Programs can simply log to `localhost` or a local file,
so you do not have to configure them to talk to an external Logstash endpoint.
This framework is suitable for non-critical logging of events which may be dropped,
such as resource usage statistics.
We currently advise using other systems for business-critical event logging,
such as PCI DSS events.

This framework tries to launch a Logstash process on every Mesos slave.
Specifically, it accepts a Mesos offer if the offered slave does not yet have Logstash running,
and the offer has enough resources to run Logstash.
This does not guarantee the presence of Logstash on every slave,
but we believe that most clusters will gain high allocation (TODO why?).


# Running

## Requirements

* A Mesos cluster at version 0.22.1 or above.
  Our scheduler and executors use version 0.25.0 of the Mesos API.

* That Mesos cluster must have the `docker` containerizer enabled.

* A Docker server must be running on every Mesos slave on port 2376,
  to be used by the `docker` containerizer.

* Every Docker server must have an API version compatible with our Docker client,
  which is currently at API version 1.20.
  This is satisfied by Docker server version 1.8.0 and above.

* The `mesos/logstash-scheduler` image.
  We maintain [releases of `mesos/logstash-scheduler` on Docker Hub](https://hub.docker.com/r/mesos/logstash-scheduler/),
  but they may not be up-to-date with this repository.
  To build the latest version, see [Building the Docker images](#building).

* Each Docker host must have access to the `mesos/logstash-executor` image,
  at the same version as your chosen `mesos/logstash-scheduler` image.
  We maintain [releases of `mesos/logstash-executor` on Docker Hub](https://hub.docker.com/r/mesos/logstash-executor/).
  To build the latest version, see [Building the Docker images](#building).


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


# Configuration

The Logstash framework is configured at the time that the scheduler is started.
To reconfigure the Logstash framework, you must tear it down and start a new one.

Each configuration option can be passed in a large number of ways:

1. Command-line arguments, e.g. `java -jar logstash-mesos-scheduler.jar --logstash.heap-size=64`
2. Environment variables, e.g. `LOGSTASH_HEAP_SIZE=64 java -jar logstash-mesos-scheduler.jar`
3. A properties file, e.g. `echo 'logstash.heap-size=64' > ./application.properties && java -jar logstash-mesos-scheduler.jar` 
4. ...

| Command-line argument            | Environment variable           | Default    | What it does                                                                                                               |
| -------------------------------- | ------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------- |
| `--zk-url=U`                     | `ZK_URL=U`                     | Required   | The Logstash framework will find Mesos using ZooKeeper at URL `U`, which must be in the format `zk://host:port/zkNode,...` |
| `--zk-timeout=T`                 | `ZK_TIMEOUT=T`                 | `20000`    | The Logstash framework will wait `T` milliseconds for ZooKeeper to respond before assuming that the session has timed out  |
| `--framework-name=N`             | `FRAMEWORK_NAME=N`             | `logstash` | The Logstash framework will show up in the Mesos Web UI with name `N`, and the ZK state will be rooted at znode `N`        |
| `--failover-timeout=T`           | `FAILOVER_TIMEOUT=T`           | `31449600` | Mesos will wait `T` seconds for the Logstash framework to failover before it kills all its tasks/executors                 |
| `--role=R`                       | `ROLE=R`                       | `*`        | The Logstash framework role will register with Mesos with framework role `U`.                                              |
| `--user=U`                       | `USER=U`                       | `root`     | Logstash tasks will be launched with Unix user `U`                                                                         |
| `--logstash.heap-size=N`         | `LOGSTASH_HEAP_SIZE=N`         | `32`       | The Logstash program will be started with `LS_HEAP_SIZE=N` FIXME what does this actually do                                |
| `--logstash.elasticsearch-url=U` | `LOGSTASH_ELASTICSEARCH_URL=U` | Absent     | If present, Logstash will forward its logs to an Elasticsearch instance at domain and port `U` FIXME this is not a URL!    |
| `--executor.cpus=C`              | `EXECUTOR_CPUS=C`              | `0.2`      | The Logstash framework will only accept resource offers with at least `C` CPUs. `C` must be a decimal greater than 0       |
| `--executor.heap-size=H`         | `EXECUTOR_HEAP_SIZE=H`         | `64`       | The memory allocation pool for the Logstash executor will be limited to `H` megabytes                                      |
| `--enable.failover=F`            | `ENABLE_FAILOVER=F`            | `true`     | If `F` is `true`, all executors and tasks will remain running after this scheduler exits FIXME what's the format for `F`   |
| `--enable.collectd=C`            | `ENABLE_COLLECTD=C`            | `false`    | If `C` is `true`, Logstash will listen for collectd events on TCP/UDP port 5000 on all executors                           |
| `--enable.syslog=S`              | `ENABLE_SYSLOG=S`              | `false`    | If `S` is `true`, Logstash will listen for syslog events on TCP port 514 on all executors                                  |
| `--enable.file=F`                | `ENABLE_FILE=F`                | `false`    | If `F` is `true`, each line in files matching the `--file.path` pattern will be treated as a log event                     |
| `--file.path=P`                  | `FILE_PATH=P`                  | `` (empty) | All files at paths matching `P`, a comma-separated list of file path glob patterns, will be watched for log lines          |

Here is an example configuration:

```
> java -jar logstash-mesos-scheduler.jar \
    --zk-url=zk://123.0.0.12:5181/logstash \
    --zk-timeout=20000 \
    --framework-name=logstash \
    --webserver-port=10000 \
    --failover-timeout=60 \
    --role='*' \
    --user=root \
    --logstash.heap-size=64 \
    --logstash.elasticsearch-url=elasticsearch.service.consul:1234 \
    --executor.cpus=0.5 \
    --executor.heap-size=128 \
    --enable.failover=false \
    --enable.collectd=true \
    --enable.syslog=true \
    --enable.file=true \
    --file.path='/var/log/*,/home/jhf/example.log'
```

Typically, you will launch the Logstash scheduler via the Docker image.
You can pass the same arguments above to your `docker run` command:

```
> docker run mesos/logstash-scheduler --zk-url=zk://123.0.0.12:5181/logstash --zk-timeout=20000 ...
```


# HTTP API

There is an HTTP API. The available endpoints are:


## Resource: `/api/configs`

A JSON array of configurations. A configuration is a JSON object with the schema:

```js
{
    "name": string, // The name of the docker image to match when,
    "input": string // The Logstash configuration segment for this framework.
}
```


### `GET /api/configs`

Returns the array of configurations.


### `POST /api/configs`

Creates a new configuration for a framework.

The new framework will be available at `/api/configs/{name}`.


## Resource: `/api/configs/{framework-name}`


### `PUT /api/configs/{framework-name}`

Updates an existing framework config. Please make sure that framework-name is proper URL encoded (e.g. in JavaScript see `encodeURIComponent`)


### `DELETE /api/configs/{framework-name}`

Removes the configuration for this framework. Please make sure that framework-name is proper URL encoded (e.g. in JavaScript see `encodeURIComponent`)


# <a name="building"></a>Building the scheduler and executor Docker images

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