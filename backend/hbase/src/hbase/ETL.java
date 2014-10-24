package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ETL {
	public static void main(String args[]) throws Exception{
		Configuration conf = HBaseConfiguration.create();  
		
		HTable hTable = new HTable(conf, args[2]);  
		Job job = new Job(conf,"hbase_load");
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		job.setInputFormatClass(TextInputFormat.class);  
		job.setOutputFormatClass(HFileOutputFormat.class);  
		job.setJarByClass(LoadMapper.class); 
		job.setMapperClass(LoadMapper.class);   
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job,new Path(args[1])); 
		HFileOutputFormat.configureIncrementalLoad(job, hTable);
		job.waitForCompletion(true);
	}
}
