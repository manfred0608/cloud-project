package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LoadMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text k = new Text();
	private Text v = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//System.out.println("Map In----------");
		if (value.getLength() == 0) return;
		String line = value.toString();
		Extract extract = Extract.create(line);
			
		if (extract != null) {
			k.set(extract.timeStamp + extract.location);
			
			for(Entry<String, Integer> entry: extract.tags){
				String tag = entry.getKey();
				int offset = entry.getValue();
				v.set(tag + ":" + extract.tid + "." + offset);
				context.write(k, v);
			}
		}
	}
}
