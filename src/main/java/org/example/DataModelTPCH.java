package org.example;

import java.sql.*;

public class DataModelTPCH {
    public static void main(String[] args) {
        try {
            Class.forName("org.duckdb.DuckDBDriver"); // Load DuckDB JDBC driver
            String dbUrl = "jdbc:duckdb:my_tpch_database.duckdb"; // Correct JDBC URL
            try (Connection conn = DriverManager.getConnection(dbUrl)) {

                // Display the top 5 entries of each table
                displayTopEntries(conn, "customer");

                System.out.println("Displayed all successfully.");


//            //Removing tables
//
//            removeTable(conn, "title_akas");
//            removeTable(conn, "title_basics");
//            removeTable(conn, "title_crew");
//            removeTable(conn, "title_episode");
//            removeTable(conn, "title_principals");
//            removeTable(conn, "title_ratings");
//            removeTable(conn, "name_basics");
//            removeTable(conn, "my_table");
//
//            System.out.println("Removed all successfully.");

            }
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB driver not found: " + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            handleSQLException(e);

        }
    }

    private static void displayTopEntries(Connection conn, String tableName) throws SQLException {
        String query = String.format("SELECT * FROM %s LIMIT 5", tableName);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            System.out.println("Top 5 entries from table: " + tableName);
            int columnCount = rs.getMetaData().getColumnCount();

            // Display column names
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rs.getMetaData().getColumnName(i) + "\t");
            }
            System.out.println();

            // Display data rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
            System.out.println("----------------------------------------------------");
        }
    }

    // Drop a table
    private static void removeTable(Connection conn, String tableName) throws SQLException {
        String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Table " + tableName + " removed.");
        }
    }


    private static void handleSQLException(SQLException e) {
        System.err.println("SQLException: " + e.getMessage());
        System.err.println("SQLState: " + e.getSQLState());
        System.err.println("VendorError: " + e.getErrorCode());
        e.printStackTrace();
    }
}
