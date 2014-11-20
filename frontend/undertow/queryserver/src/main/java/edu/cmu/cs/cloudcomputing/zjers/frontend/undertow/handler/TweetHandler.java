package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPool;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPooler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class TweetHandler implements HttpHandler {
	
	private static final String SELECT_SQL =
			"SELECT `id`, `text_censored`, `sentiment_score`" +
			"FROM `q2`" +
			"WHERE `user_id` = ? AND `created_at` = ?";

	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("userid") == null
    		|| queryParams.get("tweet_time") == null) return; 
    	
    	String userId = queryParams.get("userid").peekFirst();
    	String timeCreated = queryParams.get("tweet_time").peekFirst();
    	
//    	String SQL =
//    			"SELECT `id`, `text_censored`, `sentiment_score`" +
//    			"FROM `q2`" +
//    			"WHERE `user_id` = " + userId + " AND `created_at` = '" + timeCreated + "'";
    	
//    	System.out.println(SQL); 
    	
    	Connection conn = ConnectionPooler.getDS().getConnection();
//    	Connection conn = ConnectionPool.GetConnection();
    	
//    	Statement ps = conn.createStatement();
    	
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
    	
    	ps.setString(1, userId);
    	ps.setString(2, timeCreated);
    	    	    	
    	ResultSet rs = ps.executeQuery();
    	
//    	ResultSet rs = ps.executeQuery(SQL);
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
        if (rs == null) throw new RuntimeException("RS NULL");
        
        TreeMap<Long, String> resultMap = new TreeMap<Long, String>();
        
    	while (rs.next()) {
    		StringBuilder sb = new StringBuilder();
    		sb.append(rs.getString("id")).append(":")
    			.append(rs.getString("sentiment_score")).append(":")
    			.append(rs.getString("text_censored")).append("\n");
    		resultMap.put(rs.getLong("id"), sb.toString());
    	}
    	
    	for (Entry<Long, String> e : resultMap.entrySet()) {
    		responseSB.append(e.getValue());
    	}
    	
    	exchange.getResponseSender().send(responseSB.toString());
    	
    	rs.close();
    	ps.close();
    	conn.close();
	}

}
