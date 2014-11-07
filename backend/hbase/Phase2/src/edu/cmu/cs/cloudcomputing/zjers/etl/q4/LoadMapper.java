package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.getLength() == 0) return;
		String line = value.toString();
		Extract extract = Extract.create(line);
			
		if (extract != null) {
			Text k = new Text(extract.timeStamp + extract.location);
			
			for(String text: extract.tags){
				String offset = text.split("\\.")[1];
				String tag = text.split("\\.")[0];
				
				Text v = new Text(tag + ":" + extract.tid + "." + offset);
				
				context.write(k, v);
			}
		}
	}
}
