input {

  <#list configurations?keys as containerId >
  <#list configurations.get(containerId) as location >
  file {
    path => "${location}"
    type => "${framework.getLogType()}"
    add_field => {
      containerId => "${containerId}"
    }
  }
  </#list>
}