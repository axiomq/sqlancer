package sqlancer.presto.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class SimpleJdbcTest {

    private SimpleJdbcTest() {
    }

    static final String DB_URL = "jdbc:presto://localhost:8080/memory?SSL=false";
    static final String USER = "presto";
    static final String PASS = null;
    static final String QUERY = "SELECT current_date";

    public static void main(String[] args) throws Exception {
        // Open a connection
        ResultSet rsx = null;
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS); Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(QUERY)) {

            stmt.execute("DROP SCHEMA IF EXISTS test");
            stmt.execute("CREATE SCHEMA IF NOT EXISTS test");
            stmt.execute("USE test");

            System.out.println("Executing without \";\" at the end");
            rsx = stmt.executeQuery("SELECT current_date");
            if (rsx.next()) {
                System.out.println("got result : " + rsx.getString(1));
            }

            System.out.println("Executing with \";\" at the end");
            rsx = stmt.executeQuery("SELECT current_date;");
            rsx.close();
        } catch (SQLException e) {
            e.printStackTrace();
            if (rsx != null) {
                rsx.close();
            }
        }
    }

}
