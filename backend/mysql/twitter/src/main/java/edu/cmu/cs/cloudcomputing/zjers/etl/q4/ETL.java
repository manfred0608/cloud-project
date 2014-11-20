package edu.cmu.cs.cloudcomputing.zjers.etl.q6;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class ETL {
	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "q4etl");
		job.setJarByClass(ETL.class);

		job.setMapperClass(LoadMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(LoadReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);

		Path inPath = new Path(URI.create(args[0]));
		Path outPath = new Path(URI.create(args[1]));

		FileSystem inFS = FileSystem.get(inPath.toUri(), conf);
		FileSystem outFS = FileSystem.get(outPath.toUri(), conf);

		if (outFS.exists(outPath))
			fs.delete(outPath, true);

		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		job.waitForCompletion(true);
	}
}
