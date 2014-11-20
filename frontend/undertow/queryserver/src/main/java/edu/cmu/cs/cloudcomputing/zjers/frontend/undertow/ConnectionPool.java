package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow;

import java.sql.Connection;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPool {

	private static final String JDBC_INIT_SQL = "USE `twitter`;";
	
//	private static HikariDataSource connectionPool;
	private static BoneCP connectionPool;
	
	public static void InitializePooler()
	{
		try {
			
			BoneCPConfig config = new BoneCPConfig();
			
			config.setJdbcUrl(ServerConfig.JDBC_URL);
			config.setUsername(ServerConfig.JDBC_LOGIN);
			config.setPassword(ServerConfig.JDBC_PASSWORD);
			config.setInitSQL(JDBC_INIT_SQL);
			config.setMinConnectionsPerPartition(1);
			config.setMaxConnectionsPerPartition(3);
			config.setPartitionCount(Runtime.getRuntime().availableProcessors());
			
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
