package com.p3solutions.avrodatagenerator.base;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class AvroDataWriter {

    private final int batchSize;

    public AvroDataWriter(int batchSize) {
        this.batchSize = batchSize;
    }

    public void writeDataToAvroInParallel(Connection conn, List<Map<String, String>> metadata, Schema schema, String tableName, String filePath, CodecFactory codec) throws SQLException, IOException, InterruptedException, ExecutionException {
        String query = "SELECT * FROM " + tableName;

        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Void>> futures = new ArrayList<>();

        try (Statement stmt = conn.createStatement();
             ResultSet dataResultSet = stmt.executeQuery(query)) {

            List<GenericRecord> batch = new ArrayList<>();

            while (dataResultSet.next()) {
                GenericRecord record = new GenericData.Record(schema);
                for (int i = 0; i < metadata.size(); i++) {
                    String columnName = metadata.get(i).get("column_name");
                    String dataType = metadata.get(i).get("data_type");
                    Object value = dataResultSet.getObject(columnName);
                    record.put(columnName, value != null ? convertToAvro(value, dataType) : null);
                }

                batch.add(record);

                if (batch.size() >= batchSize) {
                    futures.add(executorService.submit(new BatchWriter(batch, filePath, schema, codec)));
                    batch = new ArrayList<>();
                }
            }

            if (!batch.isEmpty()) {
                futures.add(executorService.submit(new BatchWriter(batch, filePath, schema, codec)));
            }

            for (Future<Void> future : futures) {
                future.get();
            }

        } finally {
            executorService.shutdown();
        }
    }

    private static class BatchWriter implements Callable<Void> {
        private final List<GenericRecord> batch;
        private final String filePath;
        private final Schema schema;
        private final CodecFactory codec;

        public BatchWriter(List<GenericRecord> batch, String filePath, Schema schema, CodecFactory codec) {
            this.batch = batch;
            this.filePath = filePath;
            this.schema = schema;
            this.codec = codec;
        }

        @Override
        public Void call() throws Exception {
            try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, true);  // Append mode
                 DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {

                dataFileWriter.setCodec(codec);
                if (fileOutputStream.getChannel().position() == 0) {
                    dataFileWriter.create(schema, fileOutputStream);
                }

                for (GenericRecord record : batch) {
                    dataFileWriter.append(record);
                }
            }
            return null;
        }
    }

    private static Object convertToAvro(Object value, String dataType) {
        switch (dataType) {
            case "timestamp":
            case "timestamp without time zone":
                return ((java.sql.Timestamp) value).getTime();
            case "boolean":
            case "integer":
            case "bigint":
            case "numeric":
                return value;
            case "bytea":
                return ByteBuffer.wrap((byte[]) value);
            case "date":
                return ((java.sql.Date) value).toLocalDate().toEpochDay();
            case "character varying":
            case "text":
                return value.toString();
            case "uuid":
                return value.toString();
            case "float":
            case "double":
                return value;
            default:
                return value.toString();
        }
    }
}
