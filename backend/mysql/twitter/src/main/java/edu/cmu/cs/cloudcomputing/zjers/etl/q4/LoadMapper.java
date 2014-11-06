package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Map In----------");
		String line = value.toString();
		Extract extract = Extract.create(line);
			
		if (extract != null) {
			Text k = new Text(extract.timeStamp + ";" + extract.location);
			
			for(String text: extract.tags){
				Text v = new Text(text + ":" + extract.tid);
				
				context.write(k, v);
			}
		}
	}
}
