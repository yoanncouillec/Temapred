package temapred.jobs.totalcountperdate;

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
		
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(Bytes.toBytes("count"), Bytes.toBytes("nb"), Bytes.toBytes(sum));
		context.write(key,put);
	}
}
