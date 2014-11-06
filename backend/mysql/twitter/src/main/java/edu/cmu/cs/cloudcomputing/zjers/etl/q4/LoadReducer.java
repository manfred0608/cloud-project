package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoadReducer extends
		Reducer<Text, Text, Text, NullWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, List<Long>> map = new HashMap<String, List<Long>>();

		for (Text value : values) {
			String s = value.toString();

			String[] data = s.split(":");
			String sKey = data[0];
			long sValue = Long.valueOf(data[1]);

			if (map.containsKey(data[0])) {
				map.get(sKey).add(sValue);
			} else {
				List<Long> ls = new ArrayList<Long>();
				ls.add(sValue);
				map.put(sKey, ls);
			}
		}

		for (String sKey : map.keySet()) {
			List<Long> ls = map.get(sKey);
			Collections.sort(ls);
		}

		List<Map.Entry<String, List<Long>>> list = new LinkedList<Map.Entry<String, List<Long>>>(
				map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, List<Long>>>() {
			public int compare(Map.Entry<String, List<Long>> o1,
					Map.Entry<String, List<Long>> o2) {
				return o2.getValue().size() - o1.getValue().size();
			}
		});

		for (int i = 0; i < list.size(); i++) {
			Map.Entry<String, List<Long>> entry = list.get(i);

			
			String serial = serialize(entry.getKey(), entry.getValue());

			String time = key.toString().split(";")[0];
			String location = key.toString().split(";")[1];
			
			String res = time + "\t" + location + "\t" + serial + "\t" + (i + 1) + "\n";
			context.write(new Text(res), NullWritable.get());
		}
	}

	private String serialize(String key, List<Long> list) {
		StringBuffer sb = new StringBuffer();
		sb.append(key + ":");
		for (int i = 0; i < list.size(); i++) {
			if (i == 0)
				sb.append(list.get(i));
			else
				sb.append("," + list.get(i));
		}

		return sb.toString();
	}
}
