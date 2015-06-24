input {

  <#list frameworks as framework>
  file {
    path => "${framework.getLocalLogLocation()}"
    type => "${framework.getLogType()}"
    containerId => "${framework.getId()}"
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
  stdout {
    codec => rubydebug
  }
}
