package edu.cmu.cs.cloudcomputing.zjers.etl.q6;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Extract {
	public String timeStamp;
	public long tid;
	public String location;
	public Set<Entry<String, Integer>> tags;
	private static Pattern p = Pattern.compile("\btime\b",
			Pattern.CASE_INSENSITIVE);

	public static Extract create(String jsonData) {
		Extract reVal = null;
		try {
			JSONObject obj = new JSONObject(jsonData);

			String location = null;

			if (isValid(obj, "place") && isValid(obj.getJSONObject("place"), "name")) {
				location = obj.getJSONObject("place").getString("name");
			} else {
				if(isValid(obj, "user") && isValid(obj.getJSONObject("user"), "time_zone"))
					location = obj.getJSONObject("user").getString("time_zone");
			}

			if (location == null || location.trim().equals("null")
					|| p.matcher(location).find())
				return null;

			JSONObject entities = obj.getJSONObject("entities");
			JSONArray tags = entities.getJSONArray("hashtags");

			if (tags.length() != 0) {
				if(!isValid(obj, "created_at"))
					return null;
				String date = obj.getString("created_at");

				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
				SimpleDateFormat sdf2 = new SimpleDateFormat(
						"E MMM dd HH:mm:ss ZZZZ yyyy");

				Date temp = sdf2.parse(date);
				date = sdf1.format(temp);

				if(!isValid(obj, "id_str"))
					return null;
				
				String id_str = obj.getString("id_str");
				long id = Long.valueOf(id_str);

				Map<String, Integer> map = new HashMap<String, Integer>();

				for (int i = 0; i < tags.length(); i++) {
					JSONObject tag = tags.getJSONObject(i);
					
					if(!isValid(tag, "text") || !isValid(tag, "indices"))
						return null;
					
					String text = tag.getString("text").trim();
					int index = (int) tag.getJSONArray("indices").getInt(0);

					if (!map.containsKey(text)) {
						map.put(text, index);
					}
				}
				reVal = new Extract(id, date.trim(), location.trim(),
						map.entrySet());
				return reVal;
			}
		} catch (Exception e) {
			reVal = null;
		}
		return null;
	}

	private Extract(long tid, String ts, String location,
			Set<Entry<String, Integer>> tags) {
		this.location = location;
		this.tid = tid;
		this.timeStamp = ts;
		this.tags = tags;
	}
	
	private static boolean isValid(JSONObject obj, String key) {
		try{
			String res = obj.getString(key);
			return !res.equals("null");
		}catch(JSONException e){
			return false;
		}
	}
}
