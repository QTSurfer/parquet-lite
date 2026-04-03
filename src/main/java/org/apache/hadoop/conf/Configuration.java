package org.apache.hadoop.conf;

public class Configuration {

    public Configuration() {
        this(true);
    }
    public Configuration(boolean useDefaults) {}

    public boolean getBoolean(String x, boolean y) {
        return y;
    }

    public void setBoolean(String x, boolean y) {
    }

    public int getInt(String x, int y) {
        return y;
    }

    public String get(String x) {
        return null;
    }

    public String get(String x, String defaultValue) {
        return defaultValue;
    }

    public void set(String name, String value) {
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
