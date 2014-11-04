package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapper extends Mapper<LongWritable, Text, Text, Text> {
	public static String ORIGINAL_PREFIX = "PREFIX";
	public static int PREFIX_CHAR_NUM = 6;
	
	@Override
	public void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		if (value.getLength() == 0) return;
		
		String line = value.toString();   
		Extract extract = Extract.create(line);
		
		if (extract != null){
			Text key1 = new Text(extract.originalTweeter);
			Text value1 = new Text(extract.retweeter);
			Text key2 = new Text(extract.retweeter);
			Text value2 = new Text(ORIGINAL_PREFIX + extract.originalTweeter);
			
			context.write(key1, value1);
			context.write(key2, value2);
		}
	}
}
