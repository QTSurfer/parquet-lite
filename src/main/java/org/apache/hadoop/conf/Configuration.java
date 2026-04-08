package org.apache.hadoop.conf;

import java.util.HashMap;
import java.util.Map;

public class Configuration {

    private final Map<String, String> props = new HashMap<>();

    public Configuration() {
        this(true);
    }
    public Configuration(boolean useDefaults) {}

    public boolean getBoolean(String x, boolean y) {
        String v = props.get(x);
        return v != null ? Boolean.parseBoolean(v) : y;
    }

    public void setBoolean(String x, boolean y) {
        props.put(x, Boolean.toString(y));
    }

    public int getInt(String x, int y) {
        String v = props.get(x);
        return v != null ? Integer.parseInt(v) : y;
    }

    public void setInt(String x, int y) {
        props.put(x, Integer.toString(y));
    }

    public String get(String x) {
        return props.get(x);
    }

    public String get(String x, String defaultValue) {
        return props.getOrDefault(x, defaultValue);
    }

    public void set(String name, String value) {
        props.put(name, value);
    }

    public ClassLoader getClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public Class<?> getClassByNameOrNull(String name) {
        try {
            return Class.forName(name, true, getClassLoader());
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
