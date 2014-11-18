package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Deque;
import java.util.Map;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPooler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class RetweetHandler implements HttpHandler {
	
	private static final String SELECT_SQL =
			"SELECT `response` FROM `q3`" +
			"WHERE `id`=?";
	
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("userid") == null) return; 
    	
    	String userId = queryParams.get("userid").peekFirst();
    	
    	Connection conn = ConnectionPooler.getDS().getConnection();
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
    	ps.setString(1, userId);
    	ResultSet rs = ps.executeQuery();
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
        if (rs == null) throw new RuntimeException("RS NULL");
        
//        rs.last();
//        
//        do {
//    		responseSB
//    			.append(rs.getString("response"))
//    			.append('\n');
//		} while (rs.previous());
        
        while (rs.next()) {
        	responseSB
        		.append(rs.getString("response"))
        		.append('\n');
        }
    	
    	rs.close();
    	ps.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

}
