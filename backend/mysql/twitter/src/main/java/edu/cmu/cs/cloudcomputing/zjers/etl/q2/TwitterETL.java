package edu.cmu.cs.cloudcomputing.zjers.etl.q2;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterETL extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration jobConf = new Configuration();
		jobConf.setLong("mapreduce.task.timeout", 0);
//		jobConf.set("mapred.child.java.opts", "-Xmx256m");
		
		Job job = Job.getInstance(jobConf);
		
		job.setJarByClass(TwitterETL.class);
		job.setMapperClass(TwitterETLMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem fs = FileSystem.get(jobConf);

		Path inPath = new Path(URI.create(args[0]));
		Path outPath = new Path(URI.create(args[1]));

		@SuppressWarnings("unused")
		FileSystem inFS = FileSystem.get(inPath.toUri(), jobConf);
		FileSystem outFS = FileSystem.get(outPath.toUri(), jobConf);
		
		if (outFS.exists(outPath)) fs.delete(outPath, true);
		
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {

		if (args.length < 2) {
			System.out.println("Error: Insufficient arguments.");
			System.out.println("Usage: <input> <output>");
			System.exit(1);
		}
		
		int res = ToolRunner.run(new Configuration(), new TwitterETL(), args);
        
        System.exit(res);
	}
}
