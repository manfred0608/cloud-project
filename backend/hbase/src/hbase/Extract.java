package hbase;

import twitter.censor.Censor;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Extract {
	public String tweetId;
	public String userId;
	public String timeStamp;
	public String text;
	public String score;
	
	public static Extract create(String jsonData){
		Extract reVal = null;
		try{
			JSONObject obj = new JSONObject(jsonData);
			String[] text = obj.getString("text").split(" ");
			JSONObject userObj = obj.getJSONObject("user");
			reVal = new Extract(obj.getString("id_str"),
					userObj.getString("id_str"),
					obj.getString("created_at"),
					Integer.toString(Censor.getSentimentScore(text)),
					Censor.censor(text)
					);
		}catch(JSONException e){
			reVal = null;
		}
		
		return reVal;
	}
	
	private Extract(String tId, String uId, String ts, String sc, String t){
		tweetId = tId;
		userId = uId;
		timeStamp = ts;
		score = sc;
		text = t;
	}
}
