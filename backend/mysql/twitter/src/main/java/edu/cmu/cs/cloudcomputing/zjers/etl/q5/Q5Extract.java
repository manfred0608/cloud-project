package edu.cmu.cs.cloudcomputing.zjers.etl.q5;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Q5Extract {
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
