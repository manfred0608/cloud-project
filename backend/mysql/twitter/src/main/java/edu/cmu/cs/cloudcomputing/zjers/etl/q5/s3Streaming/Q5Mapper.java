package edu.cmu.cs.cloudcomputing.zjers.etl.q5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Q5Mapper{
	private static String RETWEETED_PREFIX = "re";
	private static int PREFIX_CHAR_NUM = 2;
    
    public static void main (String args[]){
        try{
            BufferedReader br = 
                new BufferedReader(new InputStreamReader(System.in));
            String line;
            while((line = br.readLine()) != null){
                Q5Extract extract = Q5Extract.create(line);
                
                if (extract != null){
                    System.out.println(extract.user + "\t" + 1);
                    
                    if (extract.retweetedUser != null){
                        System.out.println(extract.retweetedUser + "\t" + RETWEETED_PREFIX + extract.user);
                    }
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}

class Q5Extract {
    public String user;
    public String retweetedUser;
    
    public static Q5Extract create(String jsonData){
        Q5Extract reVal = null;
        
        try{
            JSONObject obj = new JSONObject(jsonData);
            JSONObject userObj = obj.getJSONObject("user");
            reVal = new Q5Extract(userObj.getString("id_str"), null);
            JSONObject retweeted_status = obj.getJSONObject("retweeted_status");
            if (retweeted_status != null){
                JSONObject originalUser = retweeted_status.getJSONObject("user");
                reVal.setRetweetedUser(originalUser.getString("id_str"));
            }
        }catch (JSONException e){
            reVal = null;
        }
        
        return reVal;
    }
    
    private Q5Extract(String uid, String reuid){
        user = uid;
        retweetedUser = reuid;
    }
    
    private void setUser(String uid){
        user = uid;
    }
    
    private void setRetweetedUser(String reuid){
        retweetedUser = reuid;
    }
}

