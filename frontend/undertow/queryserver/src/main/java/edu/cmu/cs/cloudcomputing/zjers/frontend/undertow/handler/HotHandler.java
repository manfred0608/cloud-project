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

public class HotHandler implements HttpHandler {
	private static final String SELECT_SQL =
			"SELECT * FROM `q5`" +
			"WHERE `user_id`=?" +
			"OR `user_id`=?";
	
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		Map<String, Deque<String>> queryParams = exchange.getQueryParameters();
		    	
    	if (queryParams.get("m") == null
    		|| queryParams.get("n") == null) return; 
    	
    	String m = queryParams.get("m").peekFirst();
    	String n = queryParams.get("n").peekFirst();  
    	    	
    	Connection conn = ConnectionPooler.getDS().getConnection();
//    	Connection conn = ConnectionPool.GetConnection();
    	
    	PreparedStatement ps = conn.prepareStatement(SELECT_SQL);
    	
    	ps.setString(1, m);
    	ps.setString(2, n);
    	    	    	
    	ResultSet rs = ps.executeQuery();
    	
    	
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain; charset=utf-8");
        
        StringBuilder responseSB = new StringBuilder(ServerConfig.HEADER_STR);
                
        if (rs == null) throw new RuntimeException("RS NULL");
        
        int mScore1 = 0;
        int mScore2 = 0;
        int mScore3 = 0;
        int mTotalScore = 0;
        int nScore1 = 0;
        int nScore2 = 0;
        int nScore3 = 0;
        int nTotalScore = 0;
                
        while (rs.next()) {
        	if (rs.getString("user_id").equals(m)) {
        		mScore1 = rs.getInt("score1");
                mScore2 = rs.getInt("score2");
                mScore3 = rs.getInt("score3");
                mTotalScore = rs.getInt("total_score");
        	} else {
                nScore1 = rs.getInt("score1");
                nScore2 = rs.getInt("score2");
                nScore3 = rs.getInt("score3");
                nTotalScore = rs.getInt("total_score");
        	}
        }

        responseSB.append(m).append('\t').append(n).append('\t').append("WINNER").append('\n');
        responseSB.append(mScore1).append('\t').append(nScore1).append('\t').append(judge(mScore1, nScore1, m, n)).append('\n');
        responseSB.append(mScore2).append('\t').append(nScore2).append('\t').append(judge(mScore2, nScore2, m, n)).append('\n');
        responseSB.append(mScore3).append('\t').append(nScore3).append('\t').append(judge(mScore3, nScore3, m, n)).append('\n');
        responseSB.append(mTotalScore).append('\t')
        		  .append(nTotalScore).append('\t')
        		  .append(judge(mTotalScore, nTotalScore, m, n)).append('\n');
        
    	rs.close();
    	ps.close();
    	conn.close();
    	
    	exchange.getResponseSender().send(responseSB.toString());
	}
	
	private static String judge(int scoreA, int scoreB, String idA, String idB) {
		if (scoreA > scoreB) return idA;
		else if (scoreA < scoreB) return idB;
		else return "X";
	}
}
