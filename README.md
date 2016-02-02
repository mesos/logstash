# Logstash Mesos Framework

This is a [Mesos](http://mesos.apache.org/) framework for running [Logstash][logstash] in your cluster.
When another program on any Mesos agent logs an event,
it will be parsed by Logstash and sent to central log locations.
There are many ways to log an event, such as [syslog](https://en.wikipedia.org/wiki/Syslog) or writing to a log file.
Programs can simply log to `localhost` or a local file,
so you do not have to configure them to talk to an external Logstash endpoint.
This framework is suitable for non-critical logging of events which may be dropped,
such as resource usage statistics.
We currently advise using other systems for business-critical event logging,
such as PCI DSS events.

This framework tries to launch a Logstash process on every Mesos agent.
Specifically, it accepts a Mesos offer if the offered agent does not yet have Logstash running,
and the offer has enough resources to run Logstash.
This does not guarantee the presence of Logstash on every agent,
but most clusters can gain high allocation
if they reserve resources for the `logstash` role.


# Running


## Requirements

* A Mesos cluster at version 0.25.0 or above.
  Our scheduler and executors use version 0.25.0 of the Mesos API.

* On every Mesos master in the cluster,
  add `logstash` to the list of roles,
  e.g. by adding the line `logstash` to the file `/etc/mesos-master/roles`.

* If you are going to enable `syslog` monitoring,
  add TCP and UDP port `514` to the resources for the `logstash` role,
  e.g. by adding `ports(logstash):[514-514]`
  to the list in the file `/etc/mesos-slave/resources`
  on every Mesos agent in the cluster.

* If you are going to enable `collectd` monitoring,
  add TCP and UDP port `25826` to the resources for the `logstash` role,
  e.g. by adding `ports(logstash):[25826-25826]`
  to the list in the file `/etc/mesos-slave/resources`
  on every Mesos agent in the cluster.

* That Mesos cluster must have the `docker` containerizer enabled.

* A Docker server must be running on every Mesos agent on port 2376,
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


## Running directly

After satisfying the above requirements,
start the Logstash framework by starting a Docker container from your `mesos/logstash-scheduler` image,
like this:

```bash
> docker run mesos/logstash-scheduler \
    --zk-url=zk://123.0.0.12:5181/logstash \
    --zk-timeout=20000 \
    --framework-name=logstash \
    --failover-timeout=60 \
    --mesos-role='*' \
    --mesos-user=root \
    --logstash.heap-size=64 \
    --logstash.elasticsearch-url=http://elasticsearch.service.consul:1234 \
    --logstash.executor-image=mesos/logstash-executor \
    --logstash.executor-version=latest \
    --logstash.syslog-port=514 \
    --logstash.collectd-port=25826 \
    --executor.cpus=0.5 \
    --executor.heap-size=128 \
    --enable.failover=false \
    --enable.collectd=true \
    --enable.syslog=true \
    --enable.file=true \
    --executor.file-path='/var/log/*,/home/jhf/example.log'
```

To reconfigure the Logstash framework, you must tear it down and start a new one.

We support providing configuration options with command-line arguments and environment variables.
This means the following two `docker` commands are equivalent:

```bash
# Using command-line arguments
docker run mesos/logstash-scheduler --zk-url=zk://123.0.0.12:5181/logstash

# Using environment variables
docker run -e ZK_URL=zk://123.0.0.12:5181/logstash mesos/logstash-scheduler
```

Note that command-line arguments take precedence:
if both a command-line argument and an environment variable are provided,
the value of the command-line argument will be used.

We recommend using command-line arguments for options which clash with common environment variables, e.g. `USER`.
(FIXME: we should prefix environment variables to avoid this.)

Here is the full list of configuration options:

| Command-line argument            | Environment variable           | Default                   | What it does                                                                                                               |
| -------------------------------- | ------------------------------ | --------------------------| -------------------------------------------------------------------------------------------------------------------------- |
| `--zk-url=U`                     | `ZK_URL=U`                     | Required                  | The Logstash framework will find Mesos using ZooKeeper at URL `U`, which must be in the format `zk://host:port/zkNode,...` |
| `--zk-timeout=T`                 | `ZK_TIMEOUT=T`                 | `20000`                   | The Logstash framework will wait `T` milliseconds for ZooKeeper to respond before assuming that the session has timed out  |
| `--framework-name=N`             | `FRAMEWORK_NAME=N`             | `logstash`                | The Logstash framework will show up in the Mesos Web UI with name `N`, and the ZK state will be rooted at znode `N`        |
| `--failover-timeout=T`           | `FAILOVER_TIMEOUT=T`           | `31449600`                | Mesos will wait `T` seconds for the Logstash framework to failover before it kills all its tasks/executors                 |
| `--mesos-role=R`                 | `MESOS_ROLE=R`                 | `logstash`                | The Logstash framework role will register with Mesos with framework role `R`                                               |
| `--mesos-user=U`                 | `MESOS_USER=U`                 | `root`                    | Logstash tasks will be launched with Unix user `U`                                                                         |
| `--mesos-principal=P`            | `MESOS_PRINCIPAL=P`            | Absent                    | If present, the Logstash framework will authenticate with Mesos as principal `P`                                           |
| `--mesos-secret=S`               | `MESOS_SECRET=S`               | Absent                    | If present, the Logstash framework will authenticate with Mesos using secret `S`                                           |
| `--logstash.heap-size=H`         | `LOGSTASH_HEAP_SIZE=H`         | `32`                      | The memory allocation pool for the Logstash process will be limited to `H` megabytes                                       |
| `--logstash.elasticsearch-url=U` | `LOGSTASH_ELASTICSEARCH_URL=U` | Absent                    | If present, Logstash will forward its logs to an Elasticsearch instance at `U`                                             |
| `--logstash.executor-image=S`    | `LOGSTASH_EXECUTOR_IMAGE=S`    | `mesos/logstash-executor` | The Logstash framework will use the Docker image with name `S` as the executor on new Mesos Agents                         | 
| `--logstash.executor-version=S`  | `LOGSTASH_EXECUTOR_VERSION=S`  | `latest`                  | The Logstash framework will use version `S` of the Docker image on new Mesos Agents                                        |
| `--logstash.syslog-port=I`       | `LOGSTASH_SYSLOG_PORT=I`       | `514`                     | Every Mesos agent will listen for syslog messages on port `I`. Must be enabled with `--enable.syslog=true`                 |
| `--logstash.collectd-port=I`     | `LOGSTASH_COLLECTD_PORT=I`     | `25826`                   | Every Mesos agent will listen for collectd messages on port `I`. Must be enabled with `--enable.collectd=true`             |
| `--executor.cpus=C`              | `EXECUTOR_CPUS=C`              | `0.2`                     | The Logstash framework will only accept resource offers with at least `C` CPUs. `C` must be a decimal greater than 0       |
| `--executor.heap-size=H`         | `EXECUTOR_HEAP_SIZE=H`         | `64`                      | The memory allocation pool for the executor will be limited to `H` megabytes                                               |
| `--enable.failover=B`            | `ENABLE_FAILOVER=B`            | `true`                    | Iff `B` is `true`, all executors and tasks will remain running after this scheduler exits FIXME what's the format for `B`? |
| `--enable.collectd=B`            | `ENABLE_COLLECTD=B`            | `false`                   | Iff `B` is `true`, Logstash will listen for collectd events on UDP port 25826 on all executors                             |
| `--enable.syslog=B`              | `ENABLE_SYSLOG=B`              | `false`                   | Iff `B` is `true`, Logstash will listen for syslog events on TCP port 514 on all executors                                 |
| `--enable.file=B`                | `ENABLE_FILE=B`                | `false`                   | Iff `B` is `true`, each line in files matching the `--file.path` pattern will be treated as a log event                    |
| `--executor.file-path=P`         | `EXECUTOR_FILE_PATH=P`         | `` (empty)                | All files at paths matching `P`, a comma-separated list of file path glob patterns, will be watched for log lines          |


## Running as Marathon app

To run the Logstash framework as a Marathon app use the following app template (save e.g. as `logstash.json`):
You can use the `"env"` map to configure the framework with environment variables, as above.

```json
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
    "ZK_URL": "zk://123.0.0.12:5181/logstash",
    "ZK_TIMEOUT": "20000",
    "FRAMEWORK_NAME": "logstash",
    "FAILOVER_TIMEOUT": "60",
    "MESOS_ROLE": "logstash",
    "NESOS_USER": "root",
    "LOGSTASH_HEAP_SIZE": "64",
    "LOGSTASH_ELASTICSEARCH_URL": "http://elasticsearch.service.consul:1234",
    "EXECUTOR_CPUS": "0.5",
    "EXECUTOR_HEAP_SIZE": "128",
    "ENABLE_FAILOVER": "false",
    "ENABLE_COLLECTD": "true",
    "ENABLE_SYSLOG": "true",
    "ENABLE_FILE": "true",
    "EXECUTOR_FILE_PATH": "/var/log/*,/home/jhf/example.log"
  }
}
```
 
Please keep in mind that if you start the Logstash app as a Marathon app that this will start a 
scheduler on one arbitrary agent. The scheduler itself will try to start one (only one) executor 
on each node. To scale the application from within Marathon makes no sense because only one scheduler
per framework is allowed to run and the framework scales itself to all agents.  


## <a name="newversion"></a>Updating to a newer version (or reinstalling the app)

When reinstalling, you must manually go into your zookeeper ui and remove the path `/logstash/frameworkId`.
This is so that the reinstalled app will be able to register without losing the Logstash docker and agent configurations.


# <a name="building"></a>Building the artifacts

The Logstash framework consists of two artifacts:
the `mesos/logstash-scheduler` and `mesos/logstash-executor` images.
To build the images, first install these dependencies:

- Java Development Kit (Java 8)
- bash
- A Docker daemon. This can be either

  - locally, or
  
  - remotely, e.g. using [Docker-Machine](https://docs.docker.com/machine/) or boot2docker.

    If choosing this option, you need to do two things before running the tests:

    - tell your Docker client where your remote Docker daemon is.
      For example, if you are using `docker-machine`, and your machine is called `dev`,
      you can run in bash:
      
      ```bash
      eval $(docker-machine env dev)
      ```

    - Add an entry to the routing table
      to allow the scheduler, which runs outside of docker when testing, to
      communicate with the rest of the cluster inside docker.

      This command:
      
      ```bash
      sudo route -n add 172.17.0.0/16 $(docker-machine ip dev)
      ```
      
      sets up a route that allows packets to be routed from your scheduler (running locally on your
      computer) to any machine inside the subnet `172.17.0.0/16`, using your docker host as gateway.

Then run the following command from this directory,
which will build a Java `.jar` for the scheduler and the executor,
wrap them in Docker images,
and run the test suite on them:


```bash
./gradlew --info clean build
```

To test you have the two images:

```
> docker images
REPOSITORY                  TAG                        IMAGE ID            CREATED             VIRTUAL SIZE
mesos/logstash-scheduler    latest                     3230c945e0f1        2 seconds ago       498.2 MB
mesos/logstash-executor     latest                     cdbc9d56ef73        2 seconds ago       589.7 MB
...
```


# Limitations


## Disk space limitations

Log files will be streamed into local files within the `logstash-mesos` container.
This requires disk space
which is hard to estimate beforehand, since it depends on the number of available log files.

The intention is to do a best guess when allocating resources from Mesos (Work in Progress).


## Full deployment not guaranteed

This framework accepts a Mesos offer if the offered agent does not yet have Logstash running,
and the offer has enough resources to run Logstash.
This does not guarantee the presence of Logstash on every agent,
because we are dependent on Mesos giving us appropriate offers for every agent.
Most clusters can gain high allocation
if they reserve resources for the `logstash` role,
but we cannot guarantee full allocation.


## Security

There is no mechanism which ensures that the Logstash output might overlap with other
Logstash configurations. In other words: Logstash might observe one framework
and output to the same destination it's using for another framework. 


# Sponsors

This project is sponsored by [Cisco Cloud Services](http://www.cisco.com/c/en/us/solutions/cloud/overview.html).
Thank you for contributing to the Open Source community!


[logstash]: https://www.elastic.co/products/logstash