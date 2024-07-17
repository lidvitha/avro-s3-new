package com.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.Map;

public class S3SinkConfig extends AbstractConfig {
    public static final String S3_BUCKET_NAME = "s3.bucket.name";
    public static final String S3_REGION = "s3.region";
    public static final String S3_BATCH_SIZE = "s3.batch.size";
    public static final String S3_BATCH_TIME_MS = "s3.batch.time.ms";
    public static final String AWS_ACCESS_KEY_ID = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY = "aws.secret.access.key";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String SCHEMA_SUBJECT = "schema.subject";
    public static final String SCHEMA_ID = "schema.id";
    public static final String SCHEMA_REGISTRY_CAPACITY = "schema.registry.capacity";


    public S3SinkConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(AWS_ACCESS_KEY_ID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Access Key ID")
                .define(AWS_SECRET_ACCESS_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "AWS Secret Access Key")
                .define(S3_BUCKET_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "S3 Bucket Name")
                .define(S3_REGION, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "S3 Region")
                .define(S3_BATCH_SIZE, ConfigDef.Type.INT, 100, ConfigDef.Importance.MEDIUM, "Number of records to batch before writing to S3")
                .define(S3_BATCH_TIME_MS, ConfigDef.Type.LONG, 3600000, ConfigDef.Importance.MEDIUM, "Time in milliseconds to batch records before writing to S3")
                .define(SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Schema Registry URL")
                .define(SCHEMA_SUBJECT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Schema Subject")
                .define(SCHEMA_ID, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "Schema ID")
                .define(SCHEMA_REGISTRY_CAPACITY, ConfigDef.Type.INT, 1000, ConfigDef.Importance.MEDIUM, "Schema Registry Client Capacity");
    }
}
