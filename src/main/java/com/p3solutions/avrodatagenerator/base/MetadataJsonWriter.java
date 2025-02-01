package com.p3solutions.avrodatagenerator.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MetadataJsonWriter {

    public void writeMetadataToJson(List<Map<String, String>> metadata, String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String metadataJson = objectMapper.writeValueAsString(metadata);

        try (FileWriter jsonWriter = new FileWriter(filePath)) {
            jsonWriter.write(metadataJson);
        }
    }
}

