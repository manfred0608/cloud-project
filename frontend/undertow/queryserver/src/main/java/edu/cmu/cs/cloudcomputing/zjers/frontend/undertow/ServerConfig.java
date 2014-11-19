package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow;

import java.math.BigInteger;

public class ServerConfig {
	
	
	// Query handler configuration
	public static final String ID1 = "6723-4767-7958";
    public static final String ID2 = "9942-1880-5592";
    public static final String ID3 = "5052-3472-7830";
    public static final String TEAMID = "ZJers";
    public static final String HEADER_STR =
    		TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
    public static final String PUBLIC_KEY = "6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153";
    public static BigInteger denom = null;
    static {
    	denom =  new BigInteger(PUBLIC_KEY);
    }
    
    // MySQL connection pool configuration
    
//	public static final String JDBC_URL = "jdbc:mysql://54.173.136.205:3306/twitter";

	public static final String JDBC_URL = "jdbc:mysql://127.0.0.1:3306/twitter";
	public static final String JDBC_LOGIN = "twitter";
	public static final String JDBC_PASSWORD = "Qaq4yGmSpfRrFXaw";
	public static final String JDBC_INIT_SQL = "USE `twitter`;";
}
