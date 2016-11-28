package temapred.jobs.detecttrends;

import java.util.Date;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;

public class Starter extends BatchJob {

	public void runMe() throws Exception {
		HTable calculus = new HTable(HBaseConfiguration.create(), HbaseTable.calculus);
		Scan scan = new Scan();
		scan.setMaxVersions();
		ResultScanner scanner = calculus.getScanner(scan);

		for(Result r: scanner){
			for(KeyValue kv : r.list()){
				String word = Bytes.toString(kv.getRow());
				String family = Bytes.toString(kv.getFamily());
				String qualifier = Bytes.toString(kv.getQualifier());
				double value = Bytes.toDouble(kv.getValue());
				long timestamp = kv.getTimestamp();
				Date date = new Date(timestamp);

				if(kv.matchingQualifier(Bytes.toBytes("z"))){
					if(Math.abs(value) > 10){
						System.out.println(
								String.format("W: %s, z: %s, d: %s", word,value,date)
						);		
					}
				}

				/*if(kv.matchingQualifier(Bytes.toBytes("realprop"))){
					System.out.println(
							String.format("W: %s, p: %s, d: %s", word,value,date)
					);	
				}*/
			}
		}
	}
}
