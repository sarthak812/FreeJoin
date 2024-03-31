package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataModel {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        Class.forName("org.duckdb.DuckDBDriver");
        conn = DriverManager.getConnection("jdbc:duckdb:");
        stmt = conn.createStatement();
        rs = stmt.executeQuery("SELECT 42");

        // Process the ResultSet here
        while (rs.next()) {
            int result = rs.getInt(1);
            System.out.println("Result: " + result);
        }

        // Close resources in the reverse order of their creation
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }
}
