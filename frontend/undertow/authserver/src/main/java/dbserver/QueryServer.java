package dbserver;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Deque;
import java.util.Map;
import java.util.Map.Entry;

public class QueryServer {

    private static final String ID1 = "6723-4767-7958";
    private static final String ID2 = "9942-1880-5592";
    private static final String ID3 = "5052-3472-7830";
    private static final String TEAMID = "ZJers";
    private static final String HEADER_STR =
    		TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
    
    private static class DBHandler implements HttpHandler {

		@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
//        	if (exchange.getRequestPath().equalsIgnoreCase("/q2")) {
        	try {
            	Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
            	
            	if (queryParams.get("userid") == null
            		|| queryParams.get("tweet_time") == null) return; 
            	
            	String userId = queryParams.get("userid").peekFirst();
            	String timeCreated = queryParams.get("tweet_time").peekFirst();
            	
            	String sql = "SELECT `id`, `text_censored`, `sentiment_score` FROM "
            			+ "`tweets` WHERE user_id=" + userId
                        + " AND created_at='" + timeCreated
                        + "' ORDER BY `id` ASC;";
            	
//            	System.out.println(sql);
            	
            	Connection conn = ConnectionPooler.GetConnection();
            	Statement statement = conn.createStatement();
            	ResultSet rs = statement.executeQuery(sql);

            	
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
                
                StringBuilder responseSB = new StringBuilder(HEADER_STR);
                
                if (rs == null) throw new RuntimeException("RS NULL");
                
            	while (rs.next()) {
            		responseSB
            			.append(rs.getString("id")).append(":")
            			.append(rs.getString("sentiment_score")).append(":")
            			.append(rs.getString("text_censored")).append("\n");
            	}
            	
            	exchange.getResponseSender().send(responseSB.toString());
            	
            	rs.close();
            	statement.close();
            	ConnectionPooler.ReleaseConnection(conn);
            	
               
//        	}
        	} catch (Exception ex) {
        		ex.printStackTrace(System.err);
        	}
        
		}
    	
    }
        
    public static void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("Usage: <port> <hostname>");
			System.exit(1);
		}
		
		ConnectionPooler.InitializePooler();
		
        Undertow server = Undertow.builder()
                .addHttpListener(Integer.parseInt(args[0]), args[1])
                .setHandler(new DBHandler())
                .build();
        
        server.start();
        
//        ConnectionPooler.ReleasePooler();
	}
}
