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

filter {
  <#list frameworks as framework>
  <#if framework.hasFilterSection() >
  if containerId =~ ${framework.getId()} {
    ${framework.getFilterSection()}
  }
  </#if>
  </#list>
}

output {
  file {
    codec => "json"
    path => "/tmp/debug"
  }
}
