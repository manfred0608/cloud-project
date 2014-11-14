package edu.cmu.cs.cloudcomputing.zjers.etl.q5;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5Reducer extends Reducer<Text, Text, Text, NullWritable> {
	int INCREMENT_1 = 1;
	int INCREMENT_2 = 3;
	int INCREMENT_3 = 10;
	
	@Override
	public void reduce(Text key, Iterable<Text> inputs,
            Context context) throws IOException, InterruptedException{
		long score1 = 0;
		long score2 = 0;
		long score3 = 0;
		
		HashSet<String> retweetedBy = new HashSet<String>();
		
		for (Text in : inputs){
			String value = in.toString();
			
			if (value.startsWith(Q5Mapper.RETWEETED_PREFIX)){
				retweetedBy.add(value.substring(Q5Mapper.PREFIX_CHAR_NUM));
				score2 += INCREMENT_2;
			}
			else{
				score1 += INCREMENT_1;
			}
		}
		
		score3 = INCREMENT_3 * retweetedBy.size();
		
		String keyString = "\"" + Long.parseLong(key.toString()) + "\",";
		String output = keyString + "\"" + score1 + "\"," +
				"\"" + score2 + "\"," +
				"\"" +score3 + "\"";
		
		context.write(new Text(output), NullWritable.get());
	}
}
