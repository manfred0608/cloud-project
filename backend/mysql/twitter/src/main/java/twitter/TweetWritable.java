package twitter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TweetWritable implements DBWritable, Writable {

	private long id;
	private long user_id;
	private Timestamp created_at;
	private String text_censored;
	private int sentiment_score;
	
	public TweetWritable(long id, long user_id, Timestamp created_at,
			String text_censored, int sentiment_score) {
		this.id = id;
		this.user_id = user_id;
		this.created_at = created_at;
		this.text_censored = text_censored;
		this.sentiment_score = sentiment_score;
	}


	@Override
	public void readFields(DataInput in) throws IOException {}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		id = rs.getLong(1);
		user_id = rs.getLong(2);
		created_at = rs.getTimestamp(3);
		text_censored = rs.getString(4);
		sentiment_score = rs.getInt(5);
	}

	@Override
	public void write(DataOutput out) throws IOException {}
	
	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setLong(1, id);
		ps.setLong(2, user_id);
		ps.setTimestamp(3, created_at);
		ps.setString(4, text_censored);
		ps.setInt(5, sentiment_score);
	}
}
