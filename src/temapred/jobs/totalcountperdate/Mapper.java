package temapred.jobs.totalcountperdate;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends TableMapper<Text, IntWritable> {
	public void map(ImmutableBytesWritable key, Result value,
			Context context) throws IOException {
		List<KeyValue> list = value.list();
		try {
			for (KeyValue kv: list) {
				int count = Bytes.toInt(kv.getValue());
				context.write(new Text(value.getRow()), new IntWritable(count));
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
