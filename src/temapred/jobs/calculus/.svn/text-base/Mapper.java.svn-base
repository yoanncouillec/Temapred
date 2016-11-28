package temapred.jobs.calculus;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Mapper extends TableMapper<Text, Text> {
	public void map(ImmutableBytesWritable key, Result value,
			Context context) throws IOException {
		List<KeyValue> list = value.list();
		try {
			for (KeyValue kv: list) {
				/* Protocol: Day|Hour|Value*/
				String formatProtocol = 
					Bytes.toString(kv.getRow()) + " " + 
					Bytes.toString(kv.getFamily())  + "|" + 
					Bytes.toInt(kv.getValue());
				context.write(new Text(kv.getQualifier()), new Text(formatProtocol));
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
}
