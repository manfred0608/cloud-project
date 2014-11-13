

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class Query {
	public final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes("F");
	
	public final static String Q2_TABLE = "TwitterQ2"; 	
	public final static byte[] Q2_ID_BYTES = Bytes.toBytes("id");
	public final static byte[] Q2_TEXT_BYTES = Bytes.toBytes("text"); 
	public final static byte[] Q2_SCORE_BYTES = Bytes.toBytes("sc"); 
	
	public final static String Q3_TABLE = "TwitterQ3";
	public final static byte[] Q3_RES = Bytes.toBytes("res");
	
	public final static String Q4_TABLE = "TwitterQ4";
	public final static byte[] Q4_RES = Bytes.toBytes("ids");
	
	private static Configuration conf;  
	private static HConnection connection;
	private static String zookeeperHostIp = "172.31.8.184";
	
	public static HConnection getConn(){
		if(connection == null){
			try {
				createConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}

		return connection;
	}
	
	public static void createConnection()
		throws IOException {
		conf = HBaseConfiguration.create();  
		conf.set("hbase.zookeeper.quorum", zookeeperHostIp);
		
		connection = HConnectionManager.createConnection(conf);
	}
	
	public static void closeConnection() throws IOException{
		if(connection != null)
			connection.close();
	}
	
	public static String[] q4Query(String date, String location, int m, int n){
		String[] reVal = new String[n - m + 1];
		HTableInterface table = null;
		
		try{
			table = connection.getTable(Q4_TABLE);
			for(int i = m; i <= n; i ++){
				Get get = new Get(Bytes.toBytes(date + ";" + location + i));
				Result result = table.get(get);
				if (result != null && !result.isEmpty()){
					reVal[i - m] = Bytes.toString(result.getValue(COLUMN_FAMILY_BYTES, Q4_RES));
				}
			}
		}catch(IOException e){
			System.out.println("getting table error");
			e.printStackTrace();
			
			reVal = null;
		}
		
		if (table != null){
			try{
				table.close();
			}catch(IOException e){
				System.out.println("closing table error");
				e.printStackTrace();
			}
		}
		
		return reVal;
	}
	
	public static List<String[]> q2Query(String user, String timestamp){		
		Get get = new Get(Bytes.toBytes(user + timestamp));
		ArrayList<String[]> reVal = null;
		HTableInterface table = null;
		
		try{
			table = connection.getTable(Q2_TABLE);
			Result result = table.get(get);
			
			if (result != null && !result.isEmpty()){
				reVal = new ArrayList<String[]>();
				List<KeyValue> ids = result.getColumn(COLUMN_FAMILY_BYTES, Q2_ID_BYTES);
				List<KeyValue> texts = result.getColumn(COLUMN_FAMILY_BYTES, Q2_TEXT_BYTES);
				List<KeyValue> scores = result.getColumn(COLUMN_FAMILY_BYTES, Q2_SCORE_BYTES);
				
				int count = 0;
				for(KeyValue kvId : ids){
					String [] entry = {Bytes.toString(kvId.getValue()), 
							Bytes.toString(texts.get(count).getValue()), 
							Bytes.toString(scores.get(count).getValue())};
					reVal.add(entry);
					
					count ++;
				}
			}	
		}catch(IOException e){
			System.out.println("getting table error");
			e.printStackTrace();
			
			reVal = null;
		}
		
		if (table != null){
			try{
				table.close();
			}catch(IOException e){
				System.out.println("closing table error");
				e.printStackTrace();
			}
		}
		
		return reVal;
	}
	
	public static String q3Query(String id){
		Get get = new Get(Bytes.toBytes(id));
		String reVal = null;
		HTableInterface table = null;
		
		try{
			table = connection.getTable(Q3_TABLE);
			Result result = table.get(get);
			
			if (result != null && !result.isEmpty()){
				reVal = Bytes.toString(result.getValue(COLUMN_FAMILY_BYTES, Q3_RES));
			}
		}catch(IOException e){
			System.out.println("getting table error");
			e.printStackTrace();
			
			reVal = null;
		}
		
		if (table != null){
			try{
				table.close();
			}catch(IOException e){
				System.out.println("closing table error");
				e.printStackTrace();
			}
		}
		
		return reVal;
	}
}
