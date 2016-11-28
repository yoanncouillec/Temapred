package temapred.jobs.colocationperdate;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import temapred.utils.stemmer.Wrapper;

public class Mapper extends TableMapper<Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	
	public void map(ImmutableBytesWritable key, Result value,
			Context context) throws IOException {
		List<KeyValue> list = value.list();
		try {
			for (KeyValue kv: list) {

				String currentValue =  Bytes.toString(kv.getValue());
				String cleanStr = currentValue.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase();
				String [] toAnalyse = cleanStr.split(" ");

				for(int i = 0; i < toAnalyse.length; i++){
					if(toAnalyse[i].equals(" ")) continue;
					String stem = Wrapper.stem(toAnalyse[i]);
					if(Starter.getCommonWord().contains(stem)) continue;
					
					for(int j = 1; j < Math.min(toAnalyse.length - i, 5); j++){
						String word1 = toAnalyse[i];
						String word2 = toAnalyse[i + j];
						String stemWord2 = Wrapper.stem(word2);
						
						if(Starter.getCommonWord().contains(stemWord2)) continue;
						
						String collocation = word1 + " " + word2;
						context.write(new Text(collocation), one);
					}
				}
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
