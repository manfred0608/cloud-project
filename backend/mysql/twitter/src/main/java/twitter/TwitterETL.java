package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwitterETL extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration jobConf = new Configuration();
		jobConf.setLong("mapreduce.task.timeout", 0);
		jobConf.set("mapred.child.java.opts", "-Xmx2048m");
		
		DBConfiguration.configureDB(
				jobConf,
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://172.31.3.45:3306/twitter?characterEncoding=utf8",
				"twitter",
				"Qaq4yGmSpfRrFXaw");
		
		Job job = Job.getInstance(jobConf);
		job.setJarByClass(TwitterETL.class);
		job.setMapperClass(TwitterETLMapper.class);
		job.setMapOutputKeyClass(TweetWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(DBOutputFormat.class);
		job.setNumReduceTasks(0);
		
		DBOutputFormat.setOutput(
				job,
				"tweets",
				new String[] {
					"id",
					"user_id",
					"created_at",
					"text_censored",
					"sentiment_score"
				});
		
        FileInputFormat.addInputPath(job, new Path(args[0]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {

		if (args.length < 1) {
			System.out.println("Error: Insufficient arguments.");
			System.out.println("Usage: <input>");
			System.exit(1);
		}
		
		int res = ToolRunner.run(new Configuration(), new TwitterETL(), args);
        
        System.exit(res);
	}
}
