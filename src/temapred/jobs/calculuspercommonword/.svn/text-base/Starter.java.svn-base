package temapred.jobs.calculuspercommonword;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;

public class Starter extends BatchJob{

	public void runMe() {
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.calculusPerWord, "calculus");
		
		HbaseWrapper.launchMapReduceJob(
				"calculusperword", 
				HbaseTable.wordCountPerDate,
				HbaseTable.calculusPerWord, 
				new Scan(), 
				Starter.class, 
				Mapper.class, 
				Reducer.class, 
				Text.class, 
				DoubleWritable.class);
	}
}
