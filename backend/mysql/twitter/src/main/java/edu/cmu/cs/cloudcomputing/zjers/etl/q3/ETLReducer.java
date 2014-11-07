package edu.cmu.cs.cloudcomputing.zjers.etl.q3;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ETLReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException{
		LinkedList<String> retweeters = new LinkedList<String>();
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
		
        eliminateDuplicate(retweeters);
		
		String keyString = "\"" + key.toString() + "\",\"";
		for (int i = 0; i < retweeters.size(); i++) {
			String user = retweeters.get(i);
			StringBuilder result = new StringBuilder(keyString);
			if (originalTweeters.contains(user)) {
				result.append("(" + user + ")");
			} else {
				result.append(user);
			}
			result.append("\"");
			
			context.write(new Text(result.toString()), NullWritable.get());
		}
	}
	
	private void eliminateDuplicate(LinkedList<String> al){
        if (al.size() < 2)
            return;
        
        ListIterator<String> itr = al.listIterator();		
        String cur = itr.next();
        while (itr.hasNext()){
            String str = itr.next();
            if(cur.equals(str))
                itr.remove();
            else
                cur = str;
        }
    }
}
