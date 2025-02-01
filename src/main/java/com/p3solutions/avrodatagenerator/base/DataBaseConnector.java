package com.p3solutions.avrodatagenerator.base;

import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.util.*;
@RequiredArgsConstructor
public class DataBaseConnector {
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String tableName;


    public List<Map<String, String>> getMetadata() throws SQLException {
        String query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?";
        List<Map<String, String>> metadata = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, String> columnInfo = new HashMap<>();
                    columnInfo.put("column_name", rs.getString("column_name"));
                    columnInfo.put("data_type", rs.getString("data_type"));
                    metadata.add(columnInfo);
                }
            }
        }
        return metadata;
    }
}

