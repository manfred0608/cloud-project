package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoadReducer extends
		Reducer<Text, Text, ImmutableBytesWritable, Put> {
	private final static String COLUMN_FAMILY = "F";
	private final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
	
	public class Tweet implements Comparable<Tweet>{
		Long id;
		int offset;
		
		Tweet(Long id, int offset){
			this.id = id;
			this.offset = offset;
		}
		
		public boolean equals(Tweet t2){
			return t2.id.equals(id);
		}

		@Override
		public int compareTo(Tweet t2) {
			// TODO Auto-generated method stub
			int diff = id.compareTo(t2.id);
			
			if(diff != 0)
				return diff;
			
			return offset - t2.offset;
		}		
	}
	
	public void reduce(Text key,
			Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Set<Tweet>> map = new HashMap<String, Set<Tweet>>();

		for (Text value : values) {
			String s = value.toString();

			String[] data = s.split(":");
			String sKey = data[0];
			String sValue = data[1];
			
			int offset = Integer.valueOf(sValue.split("\\.")[1]);
			Long id = Long.valueOf(sValue.split("\\.")[0]);

			if (map.containsKey(sKey)) {
				Set<Tweet> ls = map.get(sKey);
				
				if(!ls.contains(new Tweet(id, offset))){
					ls.add(new Tweet(id, offset));
				}
			} else {
				Set<Tweet> ls = new HashSet<Tweet>();
				ls.add(new Tweet(id, offset));
				map.put(sKey, ls);
			}
		}

		Map<String, List<Tweet>> finalmap = new HashMap<String, List<Tweet>>();
		
		for (String sKey : map.keySet()) {
			Set<Tweet> set = map.get(sKey);
			List<Tweet> tls = new ArrayList<Tweet>(set);
			Collections.sort(tls);
			finalmap.put(sKey, tls);
		}

		List<Map.Entry<String, List<Tweet>>> list = new LinkedList<Map.Entry<String, List<Tweet>>>(
				finalmap.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, List<Tweet>>>() {
			public int compare(Map.Entry<String, List<Tweet>> o1, Map.Entry<String, List<Tweet>> o2) {
				int diff = o2.getValue().size() - o1.getValue().size();
				if(diff != 0)
					return diff;
				
				int i = 0;
				
				while(i < o1.getValue().size() && i < o2.getValue().size()){
					int diff2 = o1.getValue().get(i).compareTo(o2.getValue().get(i));
					if(diff2 != 0)
						return diff2;
					i++;
				}
				return 0;
			}
		});
		
		
		
		for(int i = 0; i < list.size(); i++){
			Map.Entry<String, List<Tweet>> entry = list.get(i);
			
			byte[] newKey = Bytes.toBytes(key.toString() + (i + 1));
			byte[] newValue = Bytes.toBytes(serialize(entry.getKey(), entry.getValue()));
			ImmutableBytesWritable hKey = new ImmutableBytesWritable(newKey);
			
			Put put = new Put(newKey);
            put.add(COLUMN_FAMILY_BYTES, Bytes.toBytes("ids"), newValue);
            context.write(hKey, put);
		}
	}

	private String serialize(String key, List<Tweet> list) {
		StringBuffer sb = new StringBuffer();
		sb.append(key);
		for(int i = 0; i < list.size(); i++){
			sb.append("," + list.get(i).id);
		}
		
		return sb.toString();
	}
}
