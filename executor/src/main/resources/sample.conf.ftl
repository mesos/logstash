input {
  <#list frameworks as framework>
  file {
    path => "${framework.getLogLocation()}"
    type => "${framework.getLogType()}"
    tags => [${framework.getLogTags() ? join(", ")}]
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
