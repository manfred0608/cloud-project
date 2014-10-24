import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class Query {
	private final static String DATABASE_NAME ="TweetDB";
	
	private final static String COLUMN_FAMILY = "F";
	private final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY); 
	private final static String ID_COLUMN = "id";
	private final static byte[] ID_BYTES = Bytes.toBytes(ID_COLUMN); 
	private final static String TEXT_COLUMN = "text";
	private final static byte[] TEXT_BYTES = Bytes.toBytes(TEXT_COLUMN); 
	private final static String SCORE_COLUMN = "sc";
	private final static byte[] SCORE_BYTES = Bytes.toBytes(SCORE_COLUMN); 
	
	private static Configuration conf;  
	private static HTable hTable;
	
	public static void main(String args[]){
		
	}
	
	public static void begin(String zookeeperHostIp) throws IOException {
		conf = HBaseConfiguration.create();  
		conf.set("hbase.zookeeper.quorum", zookeeperHostIp);
		
		hTable = new HTable(conf, DATABASE_NAME);
	}
	
	public static void close() throws IOException{
		hTable.close();
	}
	
	public static List<String[]> query(String user, String timestamp) throws IOException{
		ArrayList<String[]> reVal = null;
		
		Get get = new Get(Bytes.toBytes(user + timestamp));
		Result result = hTable.get(get);
		if (result != null){
			reVal = new ArrayList<String[]>();
			List<KeyValue> ids = result.getColumn(COLUMN_FAMILY_BYTES, ID_BYTES);
			List<KeyValue> texts = result.getColumn(COLUMN_FAMILY_BYTES, TEXT_BYTES);
			List<KeyValue> scores = result.getColumn(COLUMN_FAMILY_BYTES, SCORE_BYTES);
			
			int count = 0;
			for(KeyValue kvId : ids){
				String [] entry = {Bytes.toString(kvId.getValue()), 
						Bytes.toString(texts.get(count).getValue()), 
						Bytes.toString(scores.get(count).getValue())};
				reVal.add(entry);
			}
				
			count ++;
		}
				
		return reVal;
	}
}