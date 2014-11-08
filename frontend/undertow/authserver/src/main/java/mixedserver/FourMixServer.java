package mixedserver;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Deque;
import java.util.Map;

import com.whalin.MemCached.MemCachedClient;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import dbserver.ConnectionPooler;

public class FourMixServer {
	
    private static final String ID1 = "6723-4767-7958";
    private static final String ID2 = "9942-1880-5592";
    private static final String ID3 = "5052-3472-7830";
    private static final String TEAMID = "ZJers";
    private static final String HEADER_STR =
    		TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
    private static final String PUBLIC_KEY = "6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153";
    private static BigInteger denom = null;
    static {
    	denom =  new BigInteger(PUBLIC_KEY);
    }
    
    
	
    private static class MixedHandler implements HttpHandler {

		@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			
			String path = exchange.getRequestPath();
			
        	if (exchange.getRequestPath().equalsIgnoreCase("/q1")) {
	        	Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
	        	
	        	BigInteger nominator = new BigInteger(queryParams.get("key").getFirst());
	        	String quotient = nominator.divide(denom).toString();
	        	 
	            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	            
	            StringBuffer sb = new StringBuffer();
	            sb.append(quotient).append("\n")
	            	.append(TEAMID).append(",")
	            	.append(ID1).append(",")
	            	.append(ID2).append(",")
	            	.append(ID3).append("\n")
	            	.append(sdf.format(new Date())).append("\n");
	            	
	            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
	           
	            exchange.getResponseSender().send(sb.toString());
        	} else if (exchange.getRequestPath().equalsIgnoreCase("/q2")) {
            	try {
                	Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
                	
                	if (queryParams.get("userid") == null
                		|| queryParams.get("tweet_time") == null) return; 
                	
                	String userId = queryParams.get("userid").peekFirst();
                	String timeCreated = queryParams.get("tweet_time").peekFirst();
                	
                	String sql = "SELECT `id`, `text_censored`, `sentiment_score` FROM "
                			+ "`tweets_copy` WHERE user_id=" + userId
                            + " AND created_at='" + timeCreated
                            + "' ORDER BY `id` ASC;";
                	
//                	System.out.println(sql);
                	
                	MemCachedClient mcc = new MemCachedClient();
                	Object cachedResults = mcc.get(sql);
                	
                	if (cachedResults == null) {
                	
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
    	            	mcc.set(sql, responseSB.toString());
    	            	
    	            	rs.close();
    	            	statement.close();
    	            	ConnectionPooler.ReleaseConnection(conn);
                	} else {
                		exchange.getResponseSender().send(cachedResults.toString());
                	}
                   
            	} catch (Exception ex) {
            		ex.printStackTrace(System.err);
            	}
            } else if (exchange.getRequestPath().equalsIgnoreCase("/q3")) {
            	try {
                	Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
                	
                	if (queryParams.get("userid") == null) return; 
                	
                	String userId = queryParams.get("userid").peekFirst();
                	
                	String sql = "SELECT `response` FROM "
                			+ "`retweets_copy` WHERE id=" + userId
                            + ";";
                	
//                	System.out.println(sql);
                	
//                	MemCachedClient mcc = new MemCachedClient();
//                	Object cachedResults = mcc.get(sql);
                	Object cachedResults = null;
                	
                	if (cachedResults == null) {
                	
    	            	Connection conn = ConnectionPooler.GetConnection();
    	            	Statement statement = conn.createStatement();
    	            	ResultSet rs = statement.executeQuery(sql);
    	
//                    	System.out.println(sql);
    	            	
    	                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
    	                
    	                StringBuilder responseSB = new StringBuilder(HEADER_STR);
    	                
    	                if (rs == null) throw new RuntimeException("RS NULL");
    	                
    	                rs.last();
    	                
    	                do {
    	            		responseSB
    	            			.append(rs.getString("response"))
    	            			.append('\n');
    					} while (rs.previous());
    	            	
    	            	exchange.getResponseSender().send(responseSB.toString());
//    	            	mcc.set(sql, responseSB.toString());
    	            	
    	            	rs.close();
    	            	statement.close();
    	            	ConnectionPooler.ReleaseConnection(conn);
                	} else {
                		exchange.getResponseSender().send(cachedResults.toString());
                	}
                } catch (Exception ex) {
                	ex.printStackTrace(System.err);
                }
            } else if (exchange.getRequestPath().equalsIgnoreCase("/q4")) {
            	try {
            		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
                	
                	if (queryParams.get("date") == null ||
                		queryParams.get("location") == null ||
                		queryParams.get("m") == null ||
                		queryParams.get("n") == null) return; 
                	
                	String date = queryParams.get("date").peekFirst();
                	String location = queryParams.get("location").peekFirst();
                	String m = queryParams.get("m").peekFirst();
                	String n = queryParams.get("n").peekFirst();
                	
                	String sql = "SELECT `hashtag`, `tweet_id` FROM "
                			+ "`hashtags` WHERE dateLocation=" + date + location
                			+ "AND `rank` >= m AND `rank` <= n"
                            + "ORDER BY `rank` ASC;";
                	
                	Connection conn = ConnectionPooler.GetConnection();
	            	Statement statement = conn.createStatement();
	            	ResultSet rs = statement.executeQuery(sql);
	            	
	            	exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
	                
	                StringBuilder responseSB = new StringBuilder(HEADER_STR);
	                
	                if (rs == null) throw new RuntimeException("RS NULL");
	                String prevHashTag = rs.getString("hashtag");
	                StringBuilder tweetIdSB = new StringBuilder();
	                	                
	                while (rs.next()) {
	                	//build response string
					};
	            	
	            	exchange.getResponseSender().send(responseSB.toString());
//	            	mcc.set(sql, responseSB.toString());
	            	
	            	rs.close();
	            	statement.close();
	            	ConnectionPooler.ReleaseConnection(conn);
	            	
            	} catch (Exception ex) {
            		ex.printStackTrace(System.err);
            	}
            } else {
            	exchange.getResponseSender().send("Undefined");
            }
		}
    }


	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("Usage: <port> <hostname>");
			System.exit(1);
		}
		
		ConnectionPooler.InitializePooler();
		
		int port = Integer.parseInt(args[0]);
		String hostName = args[1];
		
        Undertow server = Undertow.builder()
                .addHttpListener(port, hostName)
                .setHandler(new MixedHandler())
                .build();
        
        server.start();
        
//        ConnectionPooler.ReleasePooler();
	}
}
