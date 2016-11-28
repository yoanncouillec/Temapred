package temapred.jobs.calculuspercommonword;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Mapper extends TableMapper<Text, DoubleWritable> {
	public void map(ImmutableBytesWritable key, Result value,
			Context context) throws IOException {
		List<KeyValue> list = value.list();
		System.out.println(Bytes.toString(key.get()));
		System.out.println(Bytes.toString(value.getRow()));		
		Configuration conf = HBaseConfiguration.create();
		HTable totalCountPerDate = new HTable(conf, "totalCountPerDate");
		Get countPerThisDate = new Get(value.getRow());

		Result resultCount = totalCountPerDate.get(countPerThisDate);
		byte[] resultByte = resultCount.getValue(Bytes.toBytes("count"), Bytes.toBytes("nb"));
		
		try {
			for (KeyValue kv: list) {
				String word = Bytes.toString(kv.getQualifier());
				int count = Bytes.toInt(kv.getValue());
				double proportion = (double)count/(double)Bytes.toInt(resultByte);
				context.write(new Text(word), new DoubleWritable(proportion));
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
