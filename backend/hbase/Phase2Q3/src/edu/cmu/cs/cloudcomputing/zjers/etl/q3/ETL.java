package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ETL {
	public static void main(String args[]) throws Exception{
		Configuration conf = HBaseConfiguration.create();  
		
		HTable hTable = new HTable(conf, args[2]);  
		Job job = new Job(conf,"hbase_load");	
		job.setJarByClass(ETL.class); 

		job.setMapperClass(ETLMapper.class); 
		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ETLReducer.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		
		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		  	
		FileInputFormat.setInputPaths(job, args[0]);
//		FileOutputFormat.setOutputPath(job,new Path(args[1]));
//		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[2]);
		
		job.waitForCompletion(true);
	}
}
