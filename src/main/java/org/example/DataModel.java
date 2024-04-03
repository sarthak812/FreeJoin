package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

public class DataModel {
    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:src/IMDb title basics.tsv.duckdb")) {
//            createTable(conn);
//            System.out.println("Table created successfully.");

            importData(conn,"src/IMDb title basics.tsv");
            System.out.println("Parsed in");
            displayTableContents(conn);
            System.out.println("Scan complete");
        } catch (SQLException e) {
            System.err.println("SQLException: " + e.getMessage());
            System.err.println("SQLState: " + e.getSQLState());
            System.err.println("VendorError: " + e.getErrorCode());
            e.printStackTrace();
        }
    }

// Creation of Table

   private static void createTable(Connection conn) throws SQLException {
       try (Statement stmt = conn.createStatement()) {
           String createTableQuery = "CREATE TABLE my_table (" +
                   "tconst VARCHAR(255), " +
                   "titleType VARCHAR(50), " +
                   "primaryTitle VARCHAR(255), " +
                   "originalTitle VARCHAR(255), " +
                   "isAdult BOOLEAN, " +
                   "startYear INTEGER, " +
                   "endYear INTEGER, " +
                   "runtimeMinutes INTEGER, " +
                   "genres VARCHAR(255))";
           stmt.executeUpdate(createTableQuery);
           System.out.println("CREATE TABLE query executed.");
       }
   }



    // Show the Table
    private static void displayTableContents(Connection conn) throws SQLException {
        String selectQuery = "SELECT * FROM my_table";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectQuery)) {

            // Display column names
            int columnCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getMetaData().getColumnName(i) + "\t");
            }
            System.out.println();

            // Display row data
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
    }


    // Parse the Table
    private static void importData(Connection conn, String filePath) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Set QUOTE to a character that does not appear in your data, or to an empty string if quotes are not used
            String importQuery = "COPY my_table FROM '" + filePath + "' (FORMAT 'csv', DELIMITER '\t', NULL '\\N', QUOTE '')";
            stmt.execute(importQuery);
        }
    }
}
