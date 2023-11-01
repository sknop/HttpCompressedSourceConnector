package io.confluent.bootcamp.connect.http;

import org.apache.kafka.common.config.ConfigDef;

public class Configuration {

    final static String URL_CONFIG = "http.url";
    final static String USERNAME_CONFIG = "http.user";
    final static String PASSWORD_CONFIG = "http.password";
    final static String TOPIC_CONFIG = "topic";
    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "URL from which to read the compressed file ")
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Username for authentication")
            .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,"Password for authentication" )
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "The topic to publish data to");
}
