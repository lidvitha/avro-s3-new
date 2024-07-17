package com.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import java.io.ByteArrayInputStream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;

public class S3SinkTask extends SinkTask {
    private AmazonS3 s3Client;
    private String bucketName;
    private Map<String, List<SinkRecord>> topicBuffers;
    private Map<String, Long> topicLastFlushTimes;
    private Map<String, String> topicFileKeys;
    private int batchSize;
    private long batchTimeMs;
    private int eventCounter = 0;
    private SchemaRegistryClient schemaRegistry;
    private Schema avroSchema;

    @Override
    public void start(Map<String, String> props) {
        String accessKeyId = props.get(S3SinkConfig.AWS_ACCESS_KEY_ID);
        String secretAccessKey = props.get(S3SinkConfig.AWS_SECRET_ACCESS_KEY);
        bucketName = props.get(S3SinkConfig.S3_BUCKET_NAME);

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(props.get(S3SinkConfig.S3_REGION)))
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        topicBuffers = new HashMap<>();
        topicLastFlushTimes = new HashMap<>();
        topicFileKeys = new HashMap<>();

        batchSize = Integer.parseInt(props.get(S3SinkConfig.S3_BATCH_SIZE));
        batchTimeMs = Long.parseLong(props.get(S3SinkConfig.S3_BATCH_TIME_MS));

        // Initialize the Schema Registry client
        schemaRegistry = new CachedSchemaRegistryClient(
                props.get(S3SinkConfig.SCHEMA_REGISTRY_URL),
                Integer.parseInt(props.get(S3SinkConfig.SCHEMA_REGISTRY_CAPACITY))
        );

        // Load the Avro schema from Schema Registry
        try {
            avroSchema = ((AvroSchema) schemaRegistry.getSchemaBySubjectAndId(
                    props.get(S3SinkConfig.SCHEMA_SUBJECT),
                    Integer.parseInt(props.get(S3SinkConfig.SCHEMA_ID))
            )).rawSchema();
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Failed to load Avro schema from Schema Registry", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            String topic = record.topic();
            topicBuffers.computeIfAbsent(topic, k -> new ArrayList<>()).add(record);

            if (topicBuffers.get(topic).size() >= batchSize || (System.currentTimeMillis() - topicLastFlushTimes.getOrDefault(topic, 0L)) >= batchTimeMs) {
                flushRecords(topic);
                topicFileKeys.put(topic, generateFileKey());
            }
        }
    }

private void flushRecords(String topic) {
        if (!topicBuffers.get(topic).isEmpty()) {
            try {
                String key = String.format("%s/%s", topic, topicFileKeys.getOrDefault(topic, generateFileKey()));
                java.nio.file.Path tempFile = Files.createTempFile("parquet", ".parquet");

                // Write records to Parquet file
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tempFile.toString()))
                        .withSchema(avroSchema)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build()) {

                    for (SinkRecord record : topicBuffers.get(topic)) {
                        byte[] value = (byte[]) record.value();
                        GenericRecord avroRecord = convertBytesToGenericRecord(value, avroSchema);
                        writer.write(avroRecord);
                    }
                }

                // Upload Parquet file to S3
                s3Client.putObject(new PutObjectRequest(bucketName, key, tempFile.toFile()));
                topicBuffers.get(topic).clear();
                topicLastFlushTimes.put(topic, System.currentTimeMillis());
                Files.delete(tempFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private GenericRecord convertBytesToGenericRecord(byte[] value, Schema schema) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
        // Assuming the input byte array is in Avro binary format
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        GenericRecord record = new GenericData.Record(schema);
        try {
            record = datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return record;
    }


    private String generateFileKey() {
        eventCounter++;
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        return String.format("event%d-%s.parquet", eventCounter, timestamp);
    }

    @Override
    public void stop() {
        for (String topic : topicBuffers.keySet()) {
            if (!topicBuffers.get(topic).isEmpty()) {
                flushRecords(topic);
            }
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
