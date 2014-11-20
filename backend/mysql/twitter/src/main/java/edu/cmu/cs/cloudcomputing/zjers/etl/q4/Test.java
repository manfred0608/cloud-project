package edu.cmu.cs.cloudcomputing.zjers.etl.q6;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String file = "./file/input.txt";
		
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(file));

			String line;
			
			while ((line = br.readLine()) != null) {
				Extract test = Extract.create(line);
				int i = 0;
				
			}
			br.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
