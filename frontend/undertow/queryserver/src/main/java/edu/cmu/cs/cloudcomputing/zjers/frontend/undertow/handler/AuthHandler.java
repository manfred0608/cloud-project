package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Deque;
import java.util.Map;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.ServerConfig;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

public class AuthHandler implements HttpHandler {

	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
    	
    	BigInteger nominator = new BigInteger(queryParams.get("key").getFirst());
    	String quotient = nominator.divide(ServerConfig.denom).toString();
    	 
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        StringBuffer sb = new StringBuffer();
        sb.append(quotient).append("\n")
        	.append(ServerConfig.HEADER_STR)
        	.append(sdf.format(new Date())).append("\n");
        	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
       
        exchange.getResponseSender().send(sb.toString());
	}

}
