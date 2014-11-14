package edu.cmu.cs.cloudcomputing.zjers.etl.q5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q5Mapper  extends Mapper<LongWritable, Text, Text, Text> {
	public static String RETWEETED_PREFIX = "re";
	public static int PREFIX_CHAR_NUM = 2;
	
	@Override
	public void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		if (value.getLength() == 0) return;
		
		String line = value.toString();   
		Q5Extract extract = Q5Extract.create(line);

		if (extract != null){
			Text key1 = new Text(extract.user);
			Text value1 = new Text("1");
			
			context.write(key1, value1);
			
			if (extract.retweetedUser != null){
				Text key2 = new Text(extract.retweetedUser);
				Text value2 = new Text(RETWEETED_PREFIX + extract.user);
				context.write(key2, value2);
			}
		}
	}
}
