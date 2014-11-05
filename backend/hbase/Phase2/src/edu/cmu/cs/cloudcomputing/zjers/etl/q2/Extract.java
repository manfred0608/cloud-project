package edu.cmu.cs.cloudcomputing.zjers.etl.q2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.amazonaws.util.json.JSONObject;

import edu.cmu.cs.cloudcomputing.zjers.etl.q2.censor.Censor;

public class Extract {
	final String OLD_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
	final String NEW_FORMAT = "YYYY-MM-dd+HH:mm:ss";
	
	public String tweetId;
	public String userId;
	public String timeStamp;
	public String text;
	public String score;
	
	public static Extract create(String jsonData){
		Extract reVal = null;
		try{
			JSONObject obj = new JSONObject(jsonData);
			String text = obj.getString("text");
			JSONObject userObj = obj.getJSONObject("user");
			reVal = new Extract(obj.getString("id_str"),
					userObj.getString("id_str"),
					obj.getString("created_at"),
					Integer.toString(Censor.getSentimentScore(text)),
					Censor.censor(text)
					);
		}catch(Exception e){
			reVal = null;
		}
		
		return reVal;
	}
	
	private Extract(String tId, String uId, String ts, String sc, String t)
		throws ParseException{
		tweetId = tId;
		userId = uId;
		score = sc;
		text = t;
		
		SimpleDateFormat sdf = new SimpleDateFormat(OLD_FORMAT);
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+0000"));
		Date d = sdf.parse(ts);
		sdf.applyPattern(NEW_FORMAT);
		timeStamp = sdf.format(d);
	}
}
