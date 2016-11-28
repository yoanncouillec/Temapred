package temapred.jobs.wordcountperhour;

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
					String stem = Wrapper.stem(toAnalyse[i]);
					++Starter.nbWordInThisHour;
					context.write(new Text(stem), one);
				}
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
