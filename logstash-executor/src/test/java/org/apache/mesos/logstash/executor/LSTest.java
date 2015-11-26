package org.apache.mesos.logstash.executor;

import org.junit.Assert;
import org.junit.Test;

public class LSTest {
    @Test
    public void serializeNoQuoteString() {
        Assert.assertEquals("\"foo\"", LS.string("foo").serialize());
    }

    @Test
    public void serializeSingleQuoteString() {
        Assert.assertEquals("\"O'Brien\"", LS.string("O'Brien").serialize());
    }

    @Test
    public void serializeDoubleQuoteString() {
        Assert.assertEquals("'My name is \"James\"'", LS.string("My name is \"James\"").serialize());
    }

    @Test(expected=RuntimeException.class)
    public void serializeInvalidString() {
        LS.string("My name is \"O'Brien\"").serialize();
    }

    @Test
    public void serializeInteger() {
        Assert.assertEquals("5", LS.number(5).serialize());
    }

    @Test
    public void serializeDecimal() {
        Assert.assertEquals("5.5", LS.number(5.5).serialize());
    }

    @Test
    public void serializeKV() {
        Assert.assertEquals("\"port\" => 8080", LS.kv("port", LS.number(8080)).serialize());
    }

    @Test
    public void serializeArray() {
        Assert.assertEquals("[ 1, 2, 3 ]", LS.array(LS.number(1), LS.number(2), LS.number(3)).serialize());
    }

    @Test
    public void serializeMap() {
        Assert.assertEquals("{\n\"port\" => 8080\n\"domain\" => \"localhost\"\n}", LS.map(LS.kv("port", LS.number(8080)), LS.kv("domain", LS.string("localhost"))).serialize());
    }

    @Test
    public void serializePlugin() {
        Assert.assertEquals("collectd {\n\"prune_intervals\" => true\n}", LS.plugin("collectd", LS.map(LS.kv("prune_intervals", LS.bool(true)))).serialize());
    }

    @Test
    public void serializeSection() {
        Assert.assertEquals(
                "input {\n" +
                    "elasticsearch {\n" +
                        "\"hosts\" => \"localhost\"\n" +
                        "\"query\" => '{ \"query\": { \"match\": { \"statuscode\": 200 } } }'\n" +
                    "}\n" +
                "}",
                LS.section("input",
                    LS.plugin("elasticsearch", LS.map(
                        LS.kv("hosts", LS.string("localhost")),
                        LS.kv("query", LS.string("{ \"query\": { \"match\": { \"statuscode\": 200 } } }"))
                    ))
                ).serialize()
        );
    }

    @Test
    public void serializeEmptyConfig() {
        Assert.assertEquals(
                "",
                LS.config().serialize()
        );
    }

    @Test
    public void serializeConfig() {
        Assert.assertEquals(
            "input {\n" +
                "elasticsearch {\n" +
                    "\"hosts\" => \"localhost\"\n" +
                    "\"query\" => '{ \"query\": { \"match\": { \"statuscode\": 200 } } }'\n" +
                "}\n" +
            "}\n" +
            "filter {\n" +
                "elasticsearch {\n" +
                    "\"hosts\" => [ \"es-server\" ]\n" +
                    "\"query\" => \"type:start AND operation:%{[opid]}\"\n" +
                    "\"fields\" => [ \"@timestamp\", \"started\" ]\n" +
                "}\n" +
            "}\n" +
            "output {\n" +
                "s3 {\n" +
                    "\"access_key_id\" => \"crazy_key\"\n" +
                    "\"secret_access_key\" => \"monkey_access_key\"\n" +
                    "\"endpoint_region\" => \"eu-west-1\"\n" +
                    "\"bucket\" => \"boss_please_open_your_bucket\"\n" +
                    "\"size_file\" => 2048\n" +
                    "\"time_file\" => 5\n" +
                    "\"format\" => \"plain\"\n" +
                    "\"canned_acl\" => \"private\"\n" +
                "}\n" +
            "}",
            LS.config(
                LS.section("input",
                    LS.plugin("elasticsearch", LS.map(
                            LS.kv("hosts", LS.string("localhost")),
                            LS.kv("query", LS.string("{ \"query\": { \"match\": { \"statuscode\": 200 } } }"))
                    ))
                ),
                LS.section("filter",
                    LS.plugin("elasticsearch", LS.map(
                        LS.kv("hosts", LS.array(LS.string("es-server"))),
                        LS.kv("query", LS.string("type:start AND operation:%{[opid]}")),
                        LS.kv("fields", LS.array(LS.string("@timestamp"), LS.string("started")))
                    ))
                ),
                LS.section("output",
                    LS.plugin("s3", LS.map(
                        LS.kv("access_key_id", LS.string("crazy_key")),
                        LS.kv("secret_access_key", LS.string("monkey_access_key")),
                        LS.kv("endpoint_region", LS.string("eu-west-1")),
                        LS.kv("bucket", LS.string("boss_please_open_your_bucket")),
                        LS.kv("size_file", LS.number(2048)),
                        LS.kv("time_file", LS.number(5)),
                        LS.kv("format", LS.string("plain")),
                        LS.kv("canned_acl", LS.string("private"))
                    ))
                )
            ).serialize()
        );
    }
}
