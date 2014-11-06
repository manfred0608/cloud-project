package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Extract {
	public String timeStamp;
	public long tid;
	public String location;
	public String[] tags;
	
	public static Extract create(String jsonData){
		Extract reVal = null;
		try{
			System.out.println(jsonData);
			JSONObject obj = new JSONObject(jsonData);
			
			String location = obj.getString("place");
			
			if(location == "null"){
				JSONObject userObj = obj.getJSONObject("user");
				location = userObj.getString("time_zone");
				
				if(location != "null" && !location.toLowerCase().matches("\btime\b")){					
					JSONObject entities = obj.getJSONObject("entities");
					JSONArray tags = entities.getJSONArray("hashtags");
					
					if(tags.length() != 0){
						String date = obj.getString("created_at");
						
						SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
						SimpleDateFormat sdf2 = new SimpleDateFormat(
								"E MMM dd HH:mm:ss ZZZZ yyyy");
						
						Date temp = sdf2.parse(date);
						date = sdf1.format(temp);
						
						String id_str = obj.getString("id_str");
						long id = Long.valueOf(id_str);
						
						Set<String> set = new HashSet<String>();
						
						for(int i = 0; i < tags.length(); i++){
							JSONObject tag = tags.getJSONObject(i);
							String text = tag.getString("text");
							set.add(text);
						}
						reVal = new Extract(id, date, location, set.toArray(new String[set.size()]));
						return reVal;
					}
				}
			}
		}catch(JSONException e){
			reVal = null;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	
	private Extract(long tid, String ts, String location, String[] tags){
		this.location = location;
		this.tid = tid;
		this.timeStamp = ts;
		this.tags = tags;
	}
}
