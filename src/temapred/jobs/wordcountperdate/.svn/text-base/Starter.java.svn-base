package temapred.jobs.wordcountperdate;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import temapred.jobs.common.BatchJob;
import temapred.jobs.wordcountperdate.Mapper;
import temapred.jobs.wordcountperdate.Reducer;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;

/*
 * This is a simple test class in order to introduce the MapReduce and the
 * MapReduce Util class, please keep the comment inside the class
 */

public class Starter extends BatchJob {
	public void runMe() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, HbaseTable.sample);

		ResultScanner scanner = table.getScanner(new Scan());
		ArrayList<String> IdByDate = new ArrayList<String>();

		for (Result rr : scanner) {
			IdByDate.add(Bytes.toString(rr.getRow())); // get all date
		}
		
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.wordCountPerDate, "word");
		
		for(String s : IdByDate){
			byte [] start = Bytes.toBytes(s);
			HbaseWrapper.launchMapReduceJob(
					s, 
					HbaseTable.sample,
					HbaseTable.wordCountPerDate,
					new Scan(start,start), 
					Starter.class, 
					Mapper.class, 
					Reducer.class, 
					Text.class, 
					IntWritable.class);
		}
	}
}
