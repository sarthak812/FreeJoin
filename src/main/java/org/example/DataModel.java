package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class DataModel {
    public static void main(String[] args) {
        String dbUrl = "jdbc:duckdb:tables.duckdb"; //Location of duckdb database
        try (Connection conn = DriverManager.getConnection(dbUrl)) {
            // Define table schemas
            // title.akas
            String titleAkasSchema = "titleId VARCHAR(255), " +
                    "ordering INTEGER, " +
                    "title VARCHAR(255), " +
                    "region VARCHAR(255), " +
                    "language VARCHAR(255), " +
                    "types VARCHAR(255), " +  // Originally an array, stored as a comma-separated string
                    "attributes VARCHAR(255), " +  // Originally an array, stored as a comma-separated string
                    "isOriginalTitle BOOLEAN";

// title.basics
            String titleBasicsSchema = "tconst VARCHAR(255), " +
                    "titleType VARCHAR(50), " +
                    "primaryTitle VARCHAR(255), " +
                    "originalTitle VARCHAR(255), " +
                    "isAdult BOOLEAN, " +
                    "startYear INTEGER, " +
                    "endYear INTEGER, " +
                    "runtimeMinutes INTEGER, " +
                    "genres VARCHAR(255)";  // Originally an array, stored as a comma-separated string

// title.crew
            String titleCrewSchema = "tconst VARCHAR(255), " +
                    "directors VARCHAR(255), " +  // Originally an array of nconsts, stored as a comma-separated string
                    "writers VARCHAR(255)";  // Originally an array of nconsts, stored as a comma-separated string

// title.episode
            String titleEpisodeSchema = "tconst VARCHAR(255), " +
                    "parentTconst VARCHAR(255), " +
                    "seasonNumber INTEGER, " +
                    "episodeNumber INTEGER";

// title.principals
            String titlePrincipalsSchema = "tconst VARCHAR(255), " +
                    "ordering INTEGER, " +
                    "nconst VARCHAR(255), " +
                    "category VARCHAR(255), " +
                    "job VARCHAR(255), " +  // Might contain '\N'
                    "characters VARCHAR(255)";  // Might contain '\N'

// title.ratings
            String titleRatingsSchema = "tconst VARCHAR(255), " +
                    "averageRating DOUBLE, " +
                    "numVotes INTEGER";

// name.basics
            String nameBasicsSchema = "nconst VARCHAR(255), " +
                    "primaryName VARCHAR(255), " +
                    "birthYear INTEGER, " +
                    "deathYear INTEGER, " +  // Might contain '\N', can be handled as INTEGER with NULLs
                    "primaryProfession VARCHAR(255), " +  // Originally an array, stored as a comma-separated string
                    "knownForTitles VARCHAR(255)";  // Originally an array of tconsts, stored as a comma-separated string


//            // Create tables
//            createTable(conn, "title_akas", titleAkasSchema);
//            createTable(conn, "title_basics", titleBasicsSchema);
//            createTable(conn, "title_crew", titleCrewSchema);
//            createTable(conn, "title_episode", titleEpisodeSchema);
//            createTable(conn, "title_principals", titlePrincipalsSchema);
//            createTable(conn, "title_ratings", titleRatingsSchema);
//            createTable(conn, "name_basics", nameBasicsSchema);
//
//            System.out.println("Tables created successfully.");
//
//            // Import data
//            importData(conn, "src/tables/IMDb title akas.tsv", "title_akas");
//            importData(conn, "src/tables/IMDb title basics.tsv", "title_basics");
//            importData(conn, "src/tables/IMDb title crew.tsv", "title_crew");
//            importData(conn, "src/tables/IMDb title ratings.tsv", "title_ratings");
//            importData(conn, "src/tables/IMDb Principals.tsv", "title_principals");
//            importData(conn, "src/tables/IMDb name basics.tsv", "name_basics");
//            importData(conn, "src/tables/Title Episode Data.tsv", "title_episode");
//
//
//            System.out.println("Data imported successfully.");

            // Display the top 5 entries of each table
            displayTopEntries(conn, "title_akas");
            displayTopEntries(conn, "title_basics");
            displayTopEntries(conn, "title_crew");
            displayTopEntries(conn, "title_episode");
            displayTopEntries(conn, "title_principals");
            displayTopEntries(conn, "title_ratings");
            displayTopEntries(conn, "name_basics");

            System.out.println("Displayed all successfully.");


            //Removing tables

            removeTable(conn, "title_akas");
            removeTable(conn, "title_basics");
            removeTable(conn, "title_crew");
            removeTable(conn, "title_episode");
            removeTable(conn, "title_principals");
            removeTable(conn, "title_ratings");
            removeTable(conn, "name_basics");
            removeTable(conn, "my_table");

            System.out.println("Removed all successfully.");


        } catch (SQLException e) {
            handleSQLException(e);
        }
    }

    private static void createTable(Connection conn, String tableName, String schema) throws SQLException {
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, schema);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("Table " + tableName + " created.");
        }
    }

    private static void importData(Connection conn, String filePath, String tableName) throws SQLException {
        String sql = String.format("COPY %s FROM '%s' (FORMAT 'csv', DELIMITER '\t', NULL '\\N', QUOTE '')", tableName, filePath);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Data imported into " + tableName + " from " + filePath);
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

    
    // Function to fetch query plan for a given query using EXPLAIN, which would be directed to Apache
    //public static void getQueryPlan(Connection conn, String query) throws SQLException {
        //String explainQuery = "EXPLAIN " + query;
        //try (Statement stmt = conn.createStatement();
             //ResultSet rs = stmt.executeQuery(explainQuery)) {

            // Display query plan
            //System.out.println("Query Plan:");
            //while (rs.next()) {
                System.out.println(rs.getString(1));
           // }
        //}
    //}
//needs an appropriate call 
  

}
