package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class ConnectionUtils {
	
	private static final Map<String, Connection> connectionPool = new HashMap<>();
	
	static {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			
			@Override
			public void run() {
				for(Entry<String, Connection> entry : connectionPool.entrySet()) {
					try {
						System.out.println("Closing connection: " + entry.getKey());
						close(entry.getValue());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
		}));
	}
	
	public static Connection connection(String db, String user, String password) {
		
		String key = connectionPoolKey(db, user, password);
		return connection(db, user, password, key);
	}
	
	public static Connection connection(String db, String user, String password, String key) {
		
		Connection connection = connectionPool.get(key);
		if(connection == null) {
			connection = createConnection(db, user, password);
			connectionPool.put(key, connection);
		}
		
		return connection;
	}

	public static Connection createConnection(String db, String user, String password) {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");		
			String url = "jdbc:sqlserver://{0};database=global";
			String DBURL = MessageFormat.format(url, db);
			System.out.println("Connecting to DB:" + DBURL);
			return DriverManager.getConnection(DBURL, user, password);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String connectionPoolKey(String db, String user, String password) {
		return String.format("%s-%s-%s", db, user, password);
	}
	
	public static void close(Connection con) {
		try {
			con.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void close(List<Connection> connections) {
		try {
			for(Connection con : connections) {
				close(con);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List<String> getDatabases(Connection connection) {
		try {
			List<String> dbs = new ArrayList<>();
			ResultSet catalogs = connection.getMetaData().getCatalogs();
			while(catalogs.next()) {
				dbs.add(catalogs.getString(1));
			}
			return dbs;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static Object[] getTxLogFileDetails(Connection connection, String db) {
		String currentDb = null; 
		try {
			currentDb = connection.getCatalog();
			switchDB(connection, db, currentDb);
		
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery("select (size * 8.0)/1024.0 AS size_in_mb, name from sys.database_files where data_space_id=0");
			
			if(rs.next()) {
				double size = rs.getDouble("size_in_mb");
				String name = rs.getString("name");
				System.out.println(db + "\t\t\t" + size);
				return new Object[] {size, name};
			}
			return null;
		}  catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			switchDB(connection, db, currentDb);
		}
	}
	
	public static void shrinkTransaction(Connection connection, String db) {
		String currentDb = null;
		try {
			currentDb = connection.getCatalog();
			switchDB(connection, db, currentDb);
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery("select name from sys.database_files where type=1");
			if(rs.next()) {
				String fileName = rs.getString("name");
				
				statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(String.format("DBCC SHRINKFILE ('%s')", fileName));
				if(resultSet.next()) {
					System.out.println(db + "\t\t\t" + fileName + "\tShrink - success");
				} else {
					System.out.println(db + "\t\t\t" + fileName + "\tShrink - failure");
				}
			}
			
		} catch(Exception e) { 
			throw new RuntimeException(e);
		} finally {
			switchDB(connection, db, currentDb);
		}
	}

	public static boolean switchDB(Connection connection, String db, String currentDb) {
		
		if(!Objects.equals(db, currentDb)) {
			try {
				connection.setCatalog(db);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			return true;
		} else {
			return false;
		}
	}
}
