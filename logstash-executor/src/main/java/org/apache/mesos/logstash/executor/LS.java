package org.apache.mesos.logstash.executor;

// Since Logstash has helpfully created a new syntax with absolutely no specification or documentation,
// here is a a best-effort attempt at a syntax definition and an AST for it:

// Logstash config file syntax is:
// file ::= section*
// section ::= str "{" plugin* "}" "\n"
// plugin ::= str map
// map ::= "{" mapping* "}" "\n"
// mapping ::= string "=>" value "\n"
// value ::= map | array | string | ...
// string ::= "\"" escaped-char* "\""     // do we always need the quotes? also there must be some syntax for escaping
// array ::= "[" (value ",")* "]" "\n"

import java.util.List;

public class LS {
    public class LSFile {
        List<LSSection> sections;
        public LSFile(LSSection... sections) {

        }
    }

    public class LSSection {
        public LSSection(LSString name, LSPlugin... plugins) {
        }
    }

    public class LSPlugin {
        public LSPlugin(LSString name, LSMap config) {
        }
    }

    public interface LSValue {
    }

    public class LSMap implements LSValue {
        public LSMap(LSMapping... mappings) {
        }
    }

    public class LSMapping {
        public LSMapping(LSString key, LSValue value) {
        }
    }

    public class LSString implements LSValue {
        public LSString(String s) {
        }
    }
}

new LSSection("input",
        new LSPlugin("file", new LSMap(
            new LSMapping("path", new LSString("/var/log/messages")),
            new LSMapping("type", new LSString("syslog"))
        ),


        input {
            file {
                path => "/var/log/messages"
                type => "syslog"
            }

            file {
                path => "/var/log/apache/access.log"
                type => "apache"
            }
        }