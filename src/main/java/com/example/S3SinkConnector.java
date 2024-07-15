package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

public class S3SinkConnector extends SinkConnector {
    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return S3SinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(configProps);
    }

    @Override
    public void stop() {
        // Cleanup resources if necessary
    }

    @Override
    public ConfigDef config() {
        return S3SinkConfig.config();
    }

    @Override
    public String version() {
        return "1.0";
    }
}