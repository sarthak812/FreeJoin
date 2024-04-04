package org.example;

import org.apache.calcite.jdbc.CalciteConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionManager {

    public static CalciteConnection getCalciteConnection(String modelPath) throws SQLException, ClassNotFoundException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("model", modelPath);

        Class.forName("org.apache.calcite.jdbc.Driver");

        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        return connection.unwrap(CalciteConnection.class);
    }
}


// To establish connection in the main function
// CalciteConnection calciteConn = ConnectionManager.getCalciteConnection("/path/to/model.json");
