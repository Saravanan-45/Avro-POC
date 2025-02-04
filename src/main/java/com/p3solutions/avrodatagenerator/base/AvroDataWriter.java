package com.p3solutions.avrodatagenerator.base;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.nio.ByteBuffer;
import java.util.*;

public class AvroDataWriter {

    private final int batchSize;

    public AvroDataWriter(int batchSize) {
        this.batchSize = batchSize;
    }

    public void writeDataToAvro(Connection conn, List<Map<String, String>> metadata, Schema schema, String tableName, String filePath, CodecFactory codec) throws SQLException, IOException {
        String query = "SELECT * FROM " + tableName;

        File file = new File(filePath);
        boolean fileExists = file.exists();

        try (Statement stmt = conn.createStatement();
             ResultSet dataResultSet = stmt.executeQuery(query);
             FileOutputStream fileOutputStream = new FileOutputStream(filePath, false);
             DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {

            dataFileWriter.setCodec(codec);

            if (!fileExists || file.length() == 0) {
                dataFileWriter.create(schema, fileOutputStream);
            }

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
                    for (GenericRecord rec : batch) {
                        dataFileWriter.append(rec);
                    }
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                for (GenericRecord rec : batch) {
                    dataFileWriter.append(rec);
                }
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
            throw e;
        }


    }


    private static Object convertToAvro(Object value, String dataType) {
        if (value == null) {
            return null;
        }

        switch (dataType) {
            case "timestamp":
            case "timestamp without time zone":
                return ((java.sql.Timestamp) value).getTime();

            case "boolean":
                return ((Boolean) value);

            case "integer":
            case "bigint":
                return value;

            case "numeric":
                if (value instanceof java.math.BigDecimal) {
                    return ((java.math.BigDecimal) value).doubleValue();
                }
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
