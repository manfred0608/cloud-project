package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Deque;
import java.util.Map;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPool;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPooler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class HashtagHandler implements HttpHandler {

	private static final String SELECT_SQL =
			"SELECT `data` FROM `q4` WHERE `time_place`=?";
	
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("date") == null
    		|| queryParams.get("location") == null
    		|| queryParams.get("m") == null
    		|| queryParams.get("n") == null) return; 
    	
    	String timePlace = queryParams.get("date").peekFirst() +
    			queryParams.get("location").peekFirst();
    	String m = queryParams.get("m").peekFirst();
    	String n = queryParams.get("n").peekFirst();   
    	
    	Connection conn = ConnectionPooler.getDS().getConnection();
//    	Connection conn = ConnectionPool.GetConnection();
    	
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
//    	System.out.println(ps.toString());
    	
    	ps.setString(1, timePlace);
    	
//    	System.out.println(ps.toString());
    	    	
    	ResultSet rs = ps.executeQuery();
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
        if (rs == null) throw new RuntimeException("RS NULL");
        
        rs.next();
        responseSB.append(output(rs.getString("data"), Integer.parseInt(m), Integer.parseInt(n)));
    	
    	responseSB.append('\n');
    	
    	rs.close();
    	ps.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

	private final static String STR_SPLIT = "#";

	public static String output(String input, int m, int n){
		StringBuilder sb = new StringBuilder();

		String[] splits = input.split(STR_SPLIT);

		for (int i = 2 * m - 1; i <= 2 * n - 1; i += 2){
			sb.append(splits[i] + "\n");
		}

		return sb.toString();
	}
}
