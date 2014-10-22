package twitter;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter.censor.Censor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TwitterETLMapper extends Mapper<LongWritable, Text, TweetWritable, NullWritable> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		if (value.getLength() == 0) return;
		
		JsonParser parser = new JsonParser();
		JsonElement rootElement = parser.parse(value.toString());
		
		if (rootElement.isJsonObject()) {
			JsonObject rootNode = rootElement.getAsJsonObject();
			
			// Example: Fri Mar 21 15:10:30 +0000 2014
			SimpleDateFormat tweetDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
			Calendar c = Calendar.getInstance();
			try {
				c.setTime(tweetDateFormat.parse(rootNode.get("created_at").getAsString()));
			}
			catch (ParseException e) {
				System.err.println("Error when parsing following datetime string:");
				System.err.println(rootNode.get("created_at").getAsString());
				e.printStackTrace(System.err);
			}
			Timestamp created_at = new Timestamp(c.getTimeInMillis());
			
			long id = rootNode.get("id").getAsLong();
			
			JsonObject userNode = rootNode.getAsJsonObject("user");
			long user_id = userNode.get("id").getAsLong();
			
			String tweetText = rootNode.get("text").getAsString();
			
			String[] textWords = tweetText.split(" ");
			
			int sentiment_score = Censor.getSentimentScore(textWords);
			
			System.out.println("Before censoring: " + tweetText);
			String text_censored = Censor.censor(textWords);
			System.out.println("After censoring:" + text_censored);
			
			context.write(
					new TweetWritable(
							id,
							user_id,
							created_at,
							text_censored,
							sentiment_score),
					NullWritable.get());
		}
		
	}

	
}
