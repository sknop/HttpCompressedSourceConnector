package io.confluent.bootcamp.connect.http;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpCompressedSourceConnector extends SourceConnector {

    private Map<String, String> props;
    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpCompressedSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();

        // Only one input stream makes sense.
        configs.add(props);

        return configs;
    }

    @Override
    public void stop() {
        // nothing to do?
    }

    @Override
    public ConfigDef config() {
        return HttpCompressedSourceConfiguration.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return ExactlyOnceSupport.UNSUPPORTED;
    }
}
