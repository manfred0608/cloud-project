package edu.cmu.cs.cloudcomputing.zjers.etl.q2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ETLMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
	private final static String COLUMN_FAMILY = "F";
	private final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY); 
	private final static String TEXT_COLUMN = "text";
	private final static String SCORE_COLUMN = "sc";
	private final static String ID_COLUMN = "id";
	
	@Override
	public void map(LongWritable key, 
			Text value, 
			Context context) 
					throws IOException, InterruptedException {
		if (value.getLength() == 0) return;
		
		String line = value.toString();   
		Extract extract = Extract.create(line);
		if (extract != null){
	        byte[] rowkey = Bytes.toBytes(extract.userId + extract.timeStamp);
	        
	        ImmutableBytesWritable hKey = new ImmutableBytesWritable(rowkey);
	        
	        Put hPut = new Put(rowkey);  
	        hPut.add(COLUMN_FAMILY_BYTES, Bytes.toBytes(ID_COLUMN), Bytes.toBytes(extract.tweetId));
	        hPut.add(COLUMN_FAMILY_BYTES, Bytes.toBytes(TEXT_COLUMN), Bytes.toBytes(extract.text));
	        hPut.add(COLUMN_FAMILY_BYTES, Bytes.toBytes(SCORE_COLUMN), Bytes.toBytes(extract.score));
	        
	        context.write(hKey,hPut); 
		}
 
	}
}
