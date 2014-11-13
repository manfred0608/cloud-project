package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow;

import java.sql.Connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPooler {
	
	private static HikariDataSource ds = null;
	
	public static void InitializePooler() throws Exception
	{
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(ServerConfig.JDBC_URL);
		config.setUsername(ServerConfig.JDBC_LOGIN);
		config.setPassword(ServerConfig.JDBC_PASSWORD);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		config.addDataSourceProperty("useServerPrepStmts", "true");

		ds = new HikariDataSource(config);
		
		System.out.println("CP initialized.");
	}
	
	public static void ReleasePooler()
	{
		if(ds != null) {
			ds.shutdown();
		}
	}
	
	public static Connection GetConnection()
	{
		Connection connection = null;
		
		try {
			connection = ds.getConnection();
			System.out.println("Got connection: " + connection.toString());
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
