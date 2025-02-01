package com.p3solutions.avrodatagenerator.base;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.Schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

public class DataGenerator {

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String tableName;
    private final String metadataJsonPath;
    private final String avroDataPath;
    private final int batchSize;



    public DataGenerator(String dbUrl, String dbUser, String dbPassword, String tableName, String metadataJsonPath, String avroDataPath, int batchSize) {

        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.tableName = tableName;
        this.metadataJsonPath = metadataJsonPath;
        this.avroDataPath = avroDataPath;
        this.batchSize = batchSize;
    }

    public void execute() {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {

            DataBaseConnector metadataFetcher = new DataBaseConnector(dbUrl, dbUser, dbPassword, tableName);
            List<Map<String, String>> metadata = metadataFetcher.getMetadata();

            MetadataJsonWriter metadataJsonWriter = new MetadataJsonWriter();
            metadataJsonWriter.writeMetadataToJson(metadata, metadataJsonPath);

            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
            Schema schema = schemaGenerator.generateAvroSchema(metadata);

            AvroDataWriter avroDataWriter = new AvroDataWriter(batchSize);
            CodecFactory codec = CodecFactory.bzip2Codec();
            avroDataWriter.writeDataToAvroInParallel(conn, metadata, schema, tableName, avroDataPath, codec);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  void initializeProcess() {
        DataGenerator generator = new DataGenerator("jdbc:postgresql://localhost:5432/ads", "adsuser", "AdS@3421", "ads_app_columns", "/path/to/metadata.json", "/path/to/data.avro", 1000);
        generator.execute();
    }
}
