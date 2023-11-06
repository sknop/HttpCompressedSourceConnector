package io.confluent.bootcamp.connect.http;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HttpCompressedSourceConfiguration extends AbstractConfig {

    final static String URL_CONFIG = "http.url";
    final static String USERNAME_CONFIG = "http.user";
    final static String PASSWORD_CONFIG = "http.password";
    final static String TOPIC_CONFIG = "topic";

    final static String PAGE_SIZE_CONFIG = "page.size.lines";
    final static int PAGE_SIZE_DEFAULT = 10000;
    final static String TASK_PAUSE_CONFIG = "task.pause.ms";
    final static long TASK_PAUSE_DEFAULT = 5 * 60 * 1_000; // 5 min

    public String url;
    public String username;
    public String password;
    public String topic;
    public int pageSize;
    public long taskPause;

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "URL from which to read the compressed file ")
            .define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Username for authentication")
            .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,"Password for authentication" )
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(PAGE_SIZE_CONFIG, ConfigDef.Type.INT, PAGE_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, "Number of lines poll returns each go")
            .define(TASK_PAUSE_CONFIG, ConfigDef.Type.LONG, TASK_PAUSE_DEFAULT, ConfigDef.Importance.MEDIUM, "Task pause before returning if nothing to do");

    public HttpCompressedSourceConfiguration(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);

        url = getString(URL_CONFIG);
        username = getString(USERNAME_CONFIG);
        password = getString(PASSWORD_CONFIG);
        topic = getString(TOPIC_CONFIG);
        pageSize = getInt(PAGE_SIZE_CONFIG);
        taskPause = getLong(TASK_PAUSE_CONFIG);
    }
}
