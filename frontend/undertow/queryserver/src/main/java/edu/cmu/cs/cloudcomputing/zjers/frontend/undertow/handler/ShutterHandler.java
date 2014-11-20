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

public class ShutterHandler implements HttpHandler {

	private static final String SELECT_SQL_M =
			"SELECT `sum_prev` FROM `q6` WHERE `user_id`>=? LIMIT 1";
	private static final String SELECT_SQL_N =
			"SELECT `sum_prev` FROM `q6` WHERE `user_id`>? LIMIT 1";
	private static final String SELECT_SQL =
			"SELECT SUM(photo_count) AS sum FROM q6_mem WHERE `user_id`>=? AND `user_id`<=?";
	
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("m") == null
    		|| queryParams.get("n") == null) return; 
    	
    	String m = queryParams.get("m").peekFirst();
    	String n = queryParams.get("n").peekFirst();  
    	
//        System.out.println("M: " + m + " N: " + n);
        
        Connection conn = ConnectionPooler.getDS().getConnection();
//    	Connection conn = ConnectionPool.GetConnection();
    	
    	PreparedStatement psM = conn.prepareStatement(SELECT_SQL_M);
    	PreparedStatement psN = conn.prepareStatement(SELECT_SQL_N);
//        PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
    	
    	psM.setString(1, m);
    	psN.setString(1, n);
    	
//        System.out.println("0+ M: " + m + " N: " + n);
    	
    	ResultSet rsM = psM.executeQuery();
    	ResultSet rsN = psN.executeQuery();
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
//        System.out.println("1+ M: " + m + " N: " + n);
        
        if (rsM == null) throw new RuntimeException("RS NULL");
        
        rsM.next();
        int lo = Integer.parseInt(rsM.getString("sum_prev"));
        rsN.next();
        int hi = Integer.parseInt(rsN.getString("sum_prev"));
        responseSB.append(hi - lo).append('\n');
        
//        System.out.println("2+ M: " + m + " N: " + n);
    	
    	rsM.close();
    	rsN.close();
    	psM.close();
    	psN.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

}
