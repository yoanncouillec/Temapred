package temapred.jobs.wordcountperhour;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Reducer extends TableReducer<Text, IntWritable, Text> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		String family;
		String date;
		if(context.getJobName().contains("_")){
			int pos = context.getJobName().indexOf("_");
			date = context.getJobName().substring(0,pos);
			family = context.getJobName().substring(pos + 1,context.getJobName().length());
			System.out.println(key.toString());
			Put put = new Put(Bytes.toBytes(date));
			put.add(Bytes.toBytes(family), Bytes.toBytes(key.toString()), Bytes.toBytes(sum));
			context.write(key,put);
		}
	}
}
