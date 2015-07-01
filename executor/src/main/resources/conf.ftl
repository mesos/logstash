input {

  <#list configurations?keys as containerId >
  <#list configurations[containerId].getLogConfigurationList() as configuration >
  file {
    path => "${configuration.getLocalLogLocation()}"
    type => "${configuration.getLogType()}"
    add_field => {
      containerId => "${containerId}"
    }
  }
  </#list>
  </#list>
}