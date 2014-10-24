package authserver;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Deque;
import java.util.Map;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class QueryServer {
	
    private static final String ID1 = "6723-4767-7958";
    private static final String ID2 = "9942-1880-5592";
    private static final String ID3 = "5052-3472-7830";
    private static final String TEAMID = "ZJers";
    private static final String PUBLIC_KEY = "6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153";
    private static BigInteger denom = null;
    static {
    	denom =  new BigInteger(PUBLIC_KEY);
    }
    
	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.err.println("Usage: <port> <hostname>");
			System.exit(1);
		}
		
        Undertow server = Undertow.builder()
                .addHttpListener(Integer.parseInt(args[0]), args[1])
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(final HttpServerExchange exchange) throws Exception {
                    	
//                    	if (exchange.getRequestPath().equalsIgnoreCase("/q1")) {
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
//                    	}
                    }
                })
                .build();
        
        server.start();
	}

}
