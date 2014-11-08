package dbserver;

import java.sql.Connection;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

public class ConnectionPooler {

//	private static final String JDBC_URL = "jdbc:mysql://54.164.235.26:3306";
	private static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306";
	private static final String JDBC_LOGIN = "twitter";
	private static final String JDBC_PASSWORD = "Qaq4yGmSpfRrFXaw";
	private static final String JDBC_INIT_SQL = "USE `twitter`;";
	
	private static BoneCP connectionPool;
	
	public static void InitializePooler()
	{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
		
		try {
			BoneCPConfig config = new BoneCPConfig();
			
			config.setJdbcUrl(JDBC_URL);
			config.setUsername(JDBC_LOGIN);
			config.setPassword(JDBC_PASSWORD);
			config.setInitSQL(JDBC_INIT_SQL);
			config.setMinConnectionsPerPartition(5);
			config.setMaxConnectionsPerPartition(100);
			config.setPartitionCount(4);
			
			connectionPool = new BoneCP(config);
		} catch(Exception ex) {
			ex.printStackTrace(System.err);
		}
	}
	
	public static void ReleasePooler()
	{
		if(connectionPool != null) {
			connectionPool.shutdown();
		}
	}
	
	public static Connection GetConnection()
	{
		Connection connection = null;
		
		try {
			connection = connectionPool.getConnection();
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
		
		return connection;
	}
	
	public static void ReleaseConnection(Connection connection) 
	{
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}
}
