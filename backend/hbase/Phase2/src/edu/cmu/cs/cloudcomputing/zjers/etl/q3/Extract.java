package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Extract {
	public String retweeter;
	public String originalTweeter;
	
	public static Extract create(String jsonData){
		Extract reVal = null;
		try{
			JSONObject obj = new JSONObject(jsonData);
			JSONObject userObj = obj.getJSONObject("user");
			JSONObject retweeted_status = obj.getJSONObject("retweeted_status");
			if (retweeted_status != null){
				JSONObject originalUser = retweeted_status.getJSONObject("user");
				reVal = new Extract(userObj.getString("id_str"),
						originalUser.getString("id_str"));
			}
		}catch(JSONException e){
			reVal = null;
		}
		
		return reVal;
	}
	
	private Extract(String re, String ori){
		retweeter = re;
		originalTweeter = ori;
	}
}
