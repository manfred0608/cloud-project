package edu.cmu.cs.cloudcomputing.zjers.etl.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class Q5Reducer{
    private static String RETWEETED_PREFIX = "re";
    private static int PREFIX_CHAR_NUM = 2;
    
    private static int INCREMENT_1 = 1;
    private static int INCREMENT_2 = 3;
    private static int INCREMENT_3 = 10;
    
    public static void main(String args[]){
        try{
            BufferedReader br = 
                new BufferedReader(new InputStreamReader(System.in));
            
            String line = null;
            String currentUser = null;
            Long score1 = 0L;
            Long score2 = 0L;
            Long score3 = 0L;
            HashSet<String> retweetedBy = new HashSet<String>();
            
            while((line = br.readLine()) != null){
            	String[] parts = line.split("\t");
                String user = parts[0];
                String value = parts[1];
                
                if(currentUser != null && currentUser.equals(user)){
                    if (value.startsWith(RETWEETED_PREFIX)){
                        retweetedBy.add(value.substring(PREFIX_CHAR_NUM));
                        score2 += INCREMENT_2;
                    }
                    else{
                        score1 += INCREMENT_1;
                    }
                }
                else{
                    if(currentUser != null)
                    {
                    	score3 = (long)(INCREMENT_3 * retweetedBy.size());
                        System.out.println("\"" + currentUser + "\"," +
                                           "\"" + score1 + "\"," +
                                           "\"" + score2 + "\"," +
                                           "\"" + score3 + "\"");
                    }
                    
                    retweetedBy.clear();
                    
                    score1 = score2 = score3 = 0L;
                    
                    if (value.startsWith(RETWEETED_PREFIX)){
                        retweetedBy.add(value.substring(PREFIX_CHAR_NUM));
                        score2 += INCREMENT_2;
                    }
                    else{
                        score1 += INCREMENT_1;
                    }
                    
                    currentUser = user;
                }
            }
            
            if(currentUser != null){
            	score3 = (long)(INCREMENT_3 * retweetedBy.size());
                System.out.println("\"" + currentUser + "\"," +
                                   "\"" + score1 + "\"," +
                                   "\"" + score2 + "\"," +
                                   "\"" + score3 + "\"");
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}