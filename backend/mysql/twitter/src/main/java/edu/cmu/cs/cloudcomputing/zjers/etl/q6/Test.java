package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Test {
	public static void main(String[] args) {
		String file = "./file/15619f14twitter-parta-aa";
		Map<Long, Long> map = new HashMap<Long, Long>();
		
		
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(file));

			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				Extract test = Extract.create(line);
				if(test != null){
					if(map.containsKey(test.uid)){
						long val = map.get(test.uid);
						map.put(test.uid, val + test.count);
					}else{
						map.put(test.uid, test.count);
					}
				}
			}
			br.close();
			
			for(Long key: map.keySet())
				System.out.println(key + ":" + map.get(key));

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
