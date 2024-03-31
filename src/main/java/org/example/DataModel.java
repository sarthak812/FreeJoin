package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataModel {
    public static void main(String[] args) {
        // Try-with-resources statement to ensure proper closing of resources
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 42")) {

            // Process the ResultSet
            while (rs.next()) {
                int result = rs.getInt(1);
                System.out.println("Result: " + result);
            }
        } catch (SQLException e) {
            e.printStackTrace(); // Handle or log the exception more appropriately
        }
    }
}
