package edu.cmu.cs.cloudcomputing.zjers.etl.q4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class ETL {
	public static void main(String args[]) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = Job.getInstance(conf);

		job.setJarByClass(ETL.class);

		job.setMapperClass(LoadMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(LoadReducer.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TableOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, "hbase", TableOutputFormat.class,
				ImmutableBytesWritable.class, Put.class);

		// Defines additional sequence-file based output 'sequence' for the job
		MultipleOutputs.addNamedOutput(job, "csv",
				SequenceFileOutputFormat.class, Text.class, NullWritable.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);

		job.waitForCompletion(true);
	}
}
