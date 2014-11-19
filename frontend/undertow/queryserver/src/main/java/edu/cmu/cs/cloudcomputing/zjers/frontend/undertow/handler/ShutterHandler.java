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

public class ShutterHandler implements HttpHandler {

	private static final String SELECT_SQL =
			"SELECT SUM(`photo_count`) AS sum FROM `q6`" +
			"WHERE `user_id`>=?" +
			"AND `user_id`<=?";
	
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("m") == null
    		|| queryParams.get("n") == null) return; 
    	
    	String m = queryParams.get("m").peekFirst();
    	String n = queryParams.get("n").peekFirst();  
    	    	
    	Connection conn = ConnectionPooler.getDS().getConnection();
    	
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
    	
    	ps.setString(1, m);
    	ps.setString(2, n);
    	    	    	
    	ResultSet rs = ps.executeQuery();
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
                
        if (rs == null) throw new RuntimeException("RS NULL");
        
        responseSB.append(rs.getString("sum")).append('\n');
        
    	rs.close();
    	ps.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

}
