package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LoadReducer extends Reducer<LongWritable, LongWritable, NullWritable, Text> {
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		long total = 0;
		for (LongWritable in : values)
			total += in.get();
		
		String k = "\"" + key.toString() +"\"";
		String v = "\"" + total +"\"";
				
		context.write(NullWritable.get(), new Text(k + "," + v));
	}
}
