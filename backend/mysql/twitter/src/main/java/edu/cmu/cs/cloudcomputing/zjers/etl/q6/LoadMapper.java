package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class LoadMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (value.getLength() == 0)
			return;

		String line = value.toString();
		Extract extract = Extract.create(line);
		
		if(extract != null){			
			LongWritable k = new LongWritable(extract.uid);
			LongWritable v = new LongWritable(extract.count);
			context.write(k, v);
		}
	}
}
