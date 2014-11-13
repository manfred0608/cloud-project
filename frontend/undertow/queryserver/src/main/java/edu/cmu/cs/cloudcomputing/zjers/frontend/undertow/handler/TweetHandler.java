package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Deque;
import java.util.Map;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ConnectionPooler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class TweetHandler implements HttpHandler {
	
	private static final String SELECT_SQL =
			"SELECT `id`, `test_censored`, `sentiment_score` FROM `tweets`" +
			"WHERE `user_id` = ? AND `created_at` = ?" + 
			"ORDER BY `id` ASC";

	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	if (queryParams.get("userid") == null
    		|| queryParams.get("tweet_time") == null) return; 
    	
    	String userId = queryParams.get("userid").peekFirst();
    	String timeCreated = queryParams.get("tweet_time").peekFirst();
    	
    	System.out.println("Querying: " + userId + " " + timeCreated);
    	
    	Connection conn = ConnectionPooler.GetConnection();
//    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
//    	System.out.println(ps.toString());
//    	
//    	ps.setString(1, userId);
//    	ps.setString(2, timeCreated);
//    	
//    	System.out.println(ps.toString());
//    	    	
//    	ResultSet rs = ps.executeQuery();
    	Statement st = conn.createStatement();
    	ResultSet rs = st.executeQuery("SELECT `id`, `test_censored`, `sentiment_score` FROM `tweets`" +
			"WHERE `user_id` = " + userId + " AND `created_at` = '" + timeCreated + 
			"'ORDER BY `id` ASC");
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
        
        if (rs == null) throw new RuntimeException("RS NULL");
        
    	while (rs.next()) {
    		responseSB
    			.append(rs.getString("id")).append(":")
    			.append(rs.getString("sentiment_score")).append(":")
    			.append(rs.getString("text_censored")).append("\n");
    	}
    	
    	rs.close();
//    	ps.close();
    	st.close();
    	ConnectionPooler.ReleaseConnection(conn);
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}

}
