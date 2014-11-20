package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class Extract {
    public long count;
    public long uid;
    
    public static Extract create(String jsonData) {
        Extract reVal = null;
        try {
            JSONObject obj = new JSONObject(jsonData);
            
            String entitiesStr = obj.getString("entities");
            String userStr = obj.getString("user");
            
            if(!entitiesStr.equals("null") && !userStr.equals("null")){
                JSONObject entities = obj.getJSONObject("entities");
                JSONObject user = obj.getJSONObject("user");
                String mediaStr = entities.getString("media");
                
                String idStr = user.getString("id_str");
                
                if(!mediaStr.equals("null") && !idStr.equals("null")){
                    long count = 0;
                    JSONArray medias = entities.getJSONArray("media");
                    Long id = Long.valueOf(idStr);
                    for(int i = 0; i < medias.length(); i++){
                        JSONObject ele = medias.getJSONObject(i);
                        
                        if(ele.getString("type") != null && ele.getString("type").trim().toLowerCase().equals("photo"))
                            count++;
                    }
                    reVal = new Extract(id, count);
                    return reVal;
                }
            }			
        } catch (JSONException e) {
            reVal = null;
        }
        return null;
    }
    
    private Extract(long uid, long count) {
        this.uid = uid;
        this.count = count;
    }
}
