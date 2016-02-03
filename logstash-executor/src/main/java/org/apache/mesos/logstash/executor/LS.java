package org.apache.mesos.logstash.executor;

import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>Since Logstash has helpfully created a new syntax with absolutely no specification or documentation,
 * here is a a best-effort attempt at an AST and syntax definition.</p>
 *
 * <p>Example usage assuming :</p>
 *
 * <blockquote><pre>
 * LS.config(
 *     LS.section("input",
 *         LS.plugin("file", LS.map(
 *             LS.kv("path", LS.string("/var/log/messages")),
 *             LS.kv("type", LS.string("syslog"))
 *             ))),
 *     LS.section("output",
 *         LS.plugin("statsd", LS.map(
 *             LS.kv("increment", LS.string("apache.%{[response][status]}"))
 *         ))))
 * </pre></blockquote>
 */
public class LS {
    public static class Config {
        private final Section[] sections;
        public Config(Section... sections) {
            this.sections = sections;
        }

        public java.lang.String serialize() {
            return unlines(Arrays.stream(sections).map(Section::serialize));
        }
    }

    public static class Section {
        private final java.lang.String name;
        private final Plugin[] plugins;
        public Section(java.lang.String name, Plugin... plugins) {
            validateName(name);
            this.name = name;
            this.plugins = plugins;
        }

        public java.lang.String serialize() {
            return name + " {\n" + unlines(Arrays.stream(plugins).map(Plugin::serialize)) + "\n}";
        }
    }

    public static class Plugin implements Value {
        private final java.lang.String name;
        private final Map config;
        public Plugin(java.lang.String name, Map config) {
            validateName(name);
            this.name = name;
            this.config = config;
        }

        public java.lang.String serialize() {
            return name + " " + config.serialize();
        }
    }

    public interface Value {
        java.lang.String serialize();
    }

    public static class Map implements Value {
        private final KV[] mappings;

        public Map(KV... mappings) {
            this.mappings = mappings;
        }

        @Override
        public java.lang.String serialize() {
            return "{\n" + unlines(Arrays.stream(mappings).map(KV::serialize)) + "\n}";
        }
    }

    public static class Array implements Value {
        private final Value[] elements;

        public Array(Value... elements) {
            this.elements = elements;
        }

        @Override
        public java.lang.String serialize() {
            return "[ " + Arrays.stream(elements).map(Value::serialize).collect(Collectors.joining(", ")) + " ]";
        }
    }

    public static class KV {
        private final java.lang.String key;
        private final Value value;
        public KV(java.lang.String key, Value value) {
            this.key = key;
            this.value = value;
        }

        public java.lang.String serialize() {
            return stringToLogstashString(key) + " => " + value.serialize();
        }
    }

    public static class String implements Value {
        private final java.lang.String string;
        public String(java.lang.String string) {
            this.string = string;
        }

        public java.lang.String serialize() {
            return stringToLogstashString(string);
        }
    }

    public static class Number implements Value {
        private final double number;
        public Number(double number) {
            this.number = number;
        }
        public java.lang.String serialize() {
            return ((long) number) == number ? Long.toString((long) number) : Double.toString(number);
        }
    }

    public static class Bool implements Value {
        private final boolean bool;
        public Bool(boolean bool) {
            this.bool = bool;
        }
        public java.lang.String serialize() {
            return this.bool ? "true" : "false";
        }
    }

    public static Config config(Section... sections) {
        return new Config(sections);
    }

    public static Section section(java.lang.String name, Plugin... plugins) {
        return new Section(name, plugins);
    }

    public static Plugin plugin(java.lang.String name, Map config) {
        return new Plugin(name, config);
    }

    public static Map map(KV... mappings) {
        return new Map(mappings);
    }

    public static Array array(Value... elements) {
        return new Array(elements);
    }

    public static KV kv(java.lang.String key, Value value) {
        return new KV(key, value);
    }

    public static String string(java.lang.String s) {
        return new String(s);
    }

    public static Number number(double number) {
        return new Number(number);
    }

    public static Bool bool(boolean b) {
        return new Bool(b);
    }

    private static java.lang.String unlines(Stream<java.lang.String> lines) {
        return lines.collect(Collectors.joining("\n"));
    }

    private static java.lang.String stringToLogstashString(java.lang.String string) {
        boolean hasSingles = string.chars().anyMatch(c -> c == '\'');
        boolean hasDoubles = string.chars().anyMatch(c -> c == '"');

        if (hasSingles && hasDoubles) {
            // FIXME if and when Logstash fixes its broken syntax, we can allow these forbidden characters https://github.com/elastic/logstash/issues/1645
            throw new RuntimeException("Logstash configuration syntax cannot represent string literals with both single quotes and double quotes. This string has both: " + string);
        }
        else if (hasDoubles) {
            return "'" + string + "'";
        }
        else {
            return "\"" + string + "\"";
        }
    }

    private static Set<Integer> toCharSet(java.lang.String s) {
        return s.chars().mapToObj(Integer::new).collect(Collectors.toSet());
    }

    private static java.lang.String charSetToString(Set<Integer> s) {
        java.lang.String acc = "";
        for (Integer i : s) {
            acc = acc + Character.toChars(i.intValue());
        }
        return acc;
    }

    // Conservative (probably overly restrictive)
    private static void validateName(java.lang.String name) {
        Set<Integer> firstCharAllowedChars = toCharSet("abcdefghijklmnopqrstuvwxyz");
        Set<Integer> allowedChars = toCharSet("abcdefghijklmnopqrstuvwxyz1234567890_");

        if (name.length() == 0) {
            throw new RuntimeException("Name cannot be empty string");
        }
        else if (!firstCharAllowedChars.contains((int) name.charAt(0))) {
            throw new RuntimeException("Name cannot start with character: " + name.charAt(0));
        }
        else if (!allowedChars.containsAll(toCharSet(name.substring(1)))) {
            throw new RuntimeException("Name cannot contain characters: " + charSetToString(Sets.difference(toCharSet(name.substring(1)), allowedChars)));
        }
    }
}
