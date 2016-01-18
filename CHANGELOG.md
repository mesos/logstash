# Changelog


## Version 3 - unreleased

- ☐ DCOS certification
- ☐ Loss-less logging. Thoroughly ensure that no log messages are lost. (e.g. when a container has rolling log files)
- ☐ Enhanced Error and Failover Handling
- ☐ Support for streaming output from `docker log`
- ☐ Support for arbitrary Logstash Plugins
- ☐ Service Discovery (allow frameworks to discover the log service automatically, and configure themselves)


## Version 2 - 2015-08-14

- ☑ Basic Failover Handling with Zookeeper
- ☑ Allow reconfiguring running frameworks
- ☑ Basic DCOS compliance (Alpha stage)

  The available DCOS configuration options are documented in
  [DCOS config.json](https://github.com/triforkse/universe/blob/version-1.x/repo/packages/L/logstash/0/config.json).

  ```bash
  dcos config append package.sources "https://github.com/triforkse/universe/archive/version-1.x.zip"
  dcos package update
  dcos package install --options=logstash-options.json logstash
  ```

  The `logstash-options.json` file in the above example is where you can configure Logstash with your own settings.
  [Here is an example `logstash-options.json`](https://github.com/mesos/logstash/tree/master/dcos/logstash-options.json).
 
  Note: **Uninstalling the logstash DCOS package will shutdown the framework! See [Updating to new version](#newversion) how to preserve the your Logstash slave and docker configuations.** 


## Version 1 - 2015-07-24

- ☑ Automatic discovery of running frameworks, streaming logs from files inside the containers. (This feature has since been removed.)
- ☑ Shared Test- and Development- Setup with `mesos-elasticsearch`, `mesos-kibana`
- ☑ External LogStash Configuration (config files propagated from Master to Slaves)
- ☑ Support for outputting to Elastic Search
- ☑ Basic Error Handling
- ☑ Installation Documentation
- ☑ Design Documentation
- ☑ Configuration GUI
- ☑ REST API for managing Configurations
