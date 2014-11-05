package edu.cmu.cs.cloudcomputing.zjers.etl.q2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class ETL {
	public static void main(String args[]) throws Exception{
		Configuration conf = HBaseConfiguration.create();  
		
		Job job = Job.getInstance(conf);	
		job.setJarByClass(ETL.class); 

		job.setMapperClass(ETLMapper.class); 
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
		job.setMapOutputValueClass(Put.class);
		
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);   
		job.setOutputFormatClass(TableOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
		
		job.waitForCompletion(true);
	}
}

