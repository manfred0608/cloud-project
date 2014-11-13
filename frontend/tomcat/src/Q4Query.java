import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class Q4Query {
	public final static byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes("F");
	
	public final static String Q4_TABLE = "TwitterQ4";
	public final static byte[] Q4_RES = Bytes.toBytes("ids");
	
	private static Configuration conf;  
	private static HConnection connection;
	private static String zookeeperHostIp = "172.31.15.217";
	
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
				Get get = new Get(Bytes.toBytes(date + location + i));
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
}
