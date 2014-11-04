package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ETLReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put> {
	private final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes("F"); 
	private final static byte[] RESULT_BYTES = Bytes.toBytes("res");
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException{
		ArrayList<String> retweeters = new ArrayList<String>();
		HashSet<String> originalTweeters = new HashSet<String>();
		
		for (Text val : values){
			String user = val.toString();
			if (user.startsWith(ETLMapper.ORIGINAL_PREFIX))
				originalTweeters.add(user.substring(ETLMapper.PREFIX_CHAR_NUM));
			else
				retweeters.add(user);
		}
		
		if (retweeters.size() == 0){
			return;
		}
		
		Collections.sort(retweeters, new Comparator<String>(){
			@Override
			public int compare(String str1, String str2){
				long result = Long.parseLong(str1) - Long.parseLong(str2);
				if(result < 0)
					return -1;
				else if (result == 0)
					return 0;
				return 1;
			}
		});
		
		StringBuilder result = new StringBuilder("");
		for (int i = 0; i < retweeters.size(); i ++){
			String user = retweeters.get(i);
			if (originalTweeters.contains(user))
				result.append("(" + user + ")\n");
			else
				result.append(user + "\n");
		}
		
		byte[] rowkey = Bytes.toBytes(key.toString());
		ImmutableBytesWritable hKey = new ImmutableBytesWritable(rowkey);
		
		Put hPut = new Put(rowkey);
		hPut.add(COLUMN_FAMILY_BYTES, RESULT_BYTES, Bytes.toBytes(result.toString()));
		
		context.write(hKey, hPut);
	}
}
