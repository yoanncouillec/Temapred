package temapred.jobs.totalcountperdate;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;


public class Starter extends BatchJob{
	public void runMe() throws Exception {
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.totalCountPerDate, "count");
		
		HbaseWrapper.launchMapReduceJob(
				"stopWords", 
				HbaseTable.wordCountPerDate,
				HbaseTable.totalCountPerDate, 
				new Scan(), 
				Starter.class, 
				Mapper.class, 
				Reducer.class, 
				Text.class, 
				IntWritable.class);
	}
}
