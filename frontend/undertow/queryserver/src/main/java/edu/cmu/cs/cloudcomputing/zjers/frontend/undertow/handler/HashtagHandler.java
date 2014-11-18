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

public class HashtagHandler implements HttpHandler {

	private static final String SELECT_SQL =
			"SELECT `hashtag`, `tweet_id` FROM `q4`" +
			"WHERE `time_place`=?" +
			"AND `popularity`>=?" +
			"AND `popularity`<=?" +
			"ORDER BY `popularity` ASC";
	
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
    	
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
//    	System.out.println(ps.toString());
    	
    	ps.setString(1, timePlace);
    	ps.setString(2, m);
    	ps.setString(3, n);
    	
//    	System.out.println(ps.toString());
    	    	
    	ResultSet rs = ps.executeQuery();
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
        if (rs == null) throw new RuntimeException("RS NULL");
        
        String lastHashTag = "";
    	while (rs.next()) {
    		
    		if (lastHashTag.isEmpty() || !lastHashTag.equals(rs.getString("hashtag"))) {
    			if (!lastHashTag.isEmpty()) responseSB.append('\n');
	    		responseSB
	    			.append(rs.getString("hashtag")).append(":")
	    			.append(rs.getString("tweet_id"));
    		} else {
    			responseSB
    				.append(',')
    				.append(rs.getString("tweet_id"));
    		}
    		
    		lastHashTag = rs.getString("hashtag");
    	}
    	
    	responseSB.append('\n');
    	
    	rs.close();
    	ps.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

}
