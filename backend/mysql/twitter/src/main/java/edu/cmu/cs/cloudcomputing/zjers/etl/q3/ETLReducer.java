package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ETLReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException{
		ArrayList<String> retweeters = new ArrayList<String>();
		HashSet<String> originalTweeters = new HashSet<String>();
		
		for (Text val : values){
			String user = val.toString();
			if (user.startsWith(ETLMapper.ORIGINAL_PREFIX))
				originalTweeters.add(user.substring(ETLMapper.PREFIX_CHAR_NUM));
			else
				retweeters.add(user);
		}
		
		if (retweeters.size() == 0){
			return;
		}
		
		Collections.sort(retweeters, new Comparator<String>(){
			@Override
			public int compare(String str1, String str2){
				long result = Long.parseLong(str1) - Long.parseLong(str2);
				if(result < 0)
					return -1;
				else if (result == 0)
					return 0;
				return 1;
			}
		});
		
		StringBuilder result = new StringBuilder("\""
				+ key.toString() + "\",\"");
		for (int i = 0; i < retweeters.size(); i ++){
			String user = retweeters.get(i);
			if (originalTweeters.contains(user))
				result.append("(" + user + ")\n");
			else
				result.append(user + "\n");
		}
		
		result.append("\"");
		
		context.write(new Text(result.toString()), NullWritable.get());
	}
}
