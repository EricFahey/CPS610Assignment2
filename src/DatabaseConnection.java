import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Eric Fahey <eric.fahey@ryerson.ca>
 */
public class DatabaseConnection {

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final ServiceType serviceType;

    private Connection connection;

    public DatabaseConnection(String hostname, int port, String username, String password, ServiceType serviceType) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.serviceType = serviceType;
    }

    public boolean connect() {
        try {
            String url = getURL();
            this.connection = DriverManager.getConnection(url);
            System.out.println("Successfully Created Connection to site " + getSite() + "!");
//            this.connection.setAutoCommit(false);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public ArrayList<HashMap<String, String>> select(Tables table) throws SQLException {
        String query = "SELECT * FROM " + table.name();
        return select(query);
    }

    public ArrayList<HashMap<String, String>> selectByConditions(Tables table, String conditions) throws SQLException {
        String query = "SELECT * FROM " + table.name() + " WHERE " + conditions;
        return select(query);
    }

    public ArrayList<HashMap<String, String>> selectByAttributes(Tables table, String attributes) throws SQLException {
        String query = "SELECT " + attributes + " FROM " + table.name();
        return select(query);
    }

    public ArrayList<HashMap<String, String>> selectByAttributesAndConditions(Tables table, String attributes, String conditions) throws SQLException {
        String query = "SELECT " + attributes + " FROM " + table.name() + " WHERE " + conditions;
        return select(query);
    }

    public ArrayList<HashMap<String, String>> select(String query) throws SQLException {
        ArrayList<HashMap<String, String>> data = new ArrayList<>();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
            HashMap<String, String> mapping = new HashMap<>();
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                int columnType = resultSet.getMetaData().getColumnType(i);
                String key = resultSet.getMetaData().getColumnName(i);
                if (columnType == Types.VARCHAR) {
                    mapping.put(key, "'" + resultSet.getString(i) + "'");
                } else if (columnType == Types.FLOAT) {
                    mapping.put(key, resultSet.getFloat(i) + "");
                } else if (columnType == Types.NUMERIC) {
                    mapping.put(key, resultSet.getFloat(i) + "");
                } else {
                    System.out.println("Unhandled Column Type: " + columnType);
                }
            }
            data.add(mapping);

        }
        return data;
    }

    public void query(String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeQuery(query);
    }


    public void insert(Tables table, String values) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeQuery("INSERT INTO " + table.name() + " VALUES (" + values + ")");
    }

    public void insert(Tables table, String attributes, String values) throws SQLException {
        String query = "INSERT INTO " + table.name() + " (" + attributes + ") VALUES (" + values + ")";
        Statement statement = connection.createStatement();
        statement.executeQuery(query);
    }

    public void delete(Tables table, String conditions) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM " + table.name() + " WHERE " + conditions);
    }

    public void truncate(Tables table) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("DELETE FROM " + table.name() + " ");
    }

    public void setAutoCommitOff() {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public String getSite() {
        if (port == 1522)
            return "Central/Science";
        else if (port == 1523)
            return "Engineering";
        return "?";
    }

    public String getURL() {
        return "jdbc:oracle:thin:" + username + "/" + password + "@" + hostname + ":" + port + ":" + serviceType.getSid();
    }
}
