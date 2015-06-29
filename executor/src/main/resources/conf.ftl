input {

  <#list frameworks as framework>
  file {
    path => "${framework.getLocalLogLocation()}"
    type => "${framework.getLogType()}"
    add_field => {
      containerId => "${framework.getId()}"
    }
  }
  </#list>
}

<#list configs as config>
${config.getContent()}
</#list>