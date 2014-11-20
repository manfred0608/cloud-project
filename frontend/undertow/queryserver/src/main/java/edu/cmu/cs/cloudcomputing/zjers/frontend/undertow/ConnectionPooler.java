package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPooler {
		
	private final static HikariDataSource DS = createDataSource();
	
	private static HikariDataSource createDataSource() {

		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(ServerConfig.JDBC_URL);
		config.setUsername(ServerConfig.JDBC_LOGIN);
		config.setPassword(ServerConfig.JDBC_PASSWORD);
//		config.setConnectionTestQuery("USE `twitter`");
		config.setMaximumPoolSize(16);
		config.setReadOnly(true);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
		config.addDataSourceProperty("useServerPrepStmts", "true");
		config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		config.setCatalog("twitter");
		
		System.out.println("CP created.");

		return new HikariDataSource(config);
	}
	
	public static HikariDataSource getDS() {
		return DS;
	}
	
	private ConnectionPooler() { }
}
