package org.apache.mesos.logstash.executor;

// Since Logstash has helpfully created a new syntax with no specification or documentation, here is a a best-effort attempt at a syntax definition and an AST for it:

// Logstash config file syntax is:
// file ::= named-block*
// named-block ::= name map
// name ::= char+
// map ::= "{" mapping "}" "\n"
// mapping ::= string "=>" value "\n"
// value ::= map | array | string | ...
// string ::= "\"" escaped-char* "\""     // do we always need the quotes? also there must be some syntax for escaping
// array ::= "[" (value ",")* "]" "\n"

public class LogstashConfig {
}
