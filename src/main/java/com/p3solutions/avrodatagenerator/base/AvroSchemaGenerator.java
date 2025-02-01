package com.p3solutions.avrodatagenerator.base;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AvroSchemaGenerator {

    public Schema generateAvroSchema(List<Map<String, String>> metadata) {
        List<Schema.Field> fields = new ArrayList<>();

        for (Map<String, String> column : metadata) {
            String columnName = column.get("column_name");
            String dataType = column.get("data_type");

            Schema fieldSchema = Schema.createUnion(
                    Arrays.asList(Schema.create(Schema.Type.NULL), createFieldSchema(dataType))
            );

            fields.add(new Schema.Field(columnName, fieldSchema, null, null));
        }

        return Schema.createRecord("User", null, null, false, fields);
    }

    private Schema createFieldSchema(String dataType) {
        switch (dataType) {
            case "integer":
                return Schema.create(Schema.Type.INT);
            case "bigint":
            case "numeric":
                return Schema.create(Schema.Type.LONG);
            case "text":
            case "character varying":
                return Schema.create(Schema.Type.STRING);
            case "boolean":
                return Schema.create(Schema.Type.BOOLEAN);
            case "date":
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case "timestamp":
            case "timestamp without time zone":
                return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case "time":
                return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
            case "bytea":
                return Schema.create(Schema.Type.BYTES);
            case "uuid":
                return LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
            case "float":
                return Schema.create(Schema.Type.FLOAT);
            case "double":
                return Schema.create(Schema.Type.DOUBLE);
            default:
                return Schema.create(Schema.Type.STRING);
        }
    }
}
