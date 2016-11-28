package temapred.jobs.colocationperdate;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import temapred.jobs.common.ParallelJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;

/*
 * This is a simple test class in order to introduce the MapReduce and the
 * MapReduce Util class, please keep the comment inside the class
 */

public class Starter extends ParallelJob {
	private static ArrayList<String> commonWord = null;

	public void runMe() throws Exception {
		String sDay = "2011-04-10";
		int secondMin = 3600*4;
		int secondMax = 3600*6;
		
		Scan colScan = new Scan(Bytes.toBytes(sDay), Bytes.toBytes(sDay));
		colScan.setTimeRange(secondMin, secondMax);
		
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.colocationPerDate, "colocation");
		Job colocationPerDate = 
			HbaseWrapper.launchParallelMapReduceJob(
					sDay, 
					HbaseTable.sample,
					HbaseTable.colocationPerDate,
					colScan, 
					Starter.class, 
					Mapper.class, 
					Reducer.class, 
					Text.class, 
					IntWritable.class);

		/** L'ajout de ces deux methodes est du au faite que ici nous somme
		 * dans un job parallel, nous aurons besoin du job a l'exterieur de cette classe
		 * pour pouvoir verifier ca terminaison
		 */
		this.setJob(colocationPerDate);
		this.getJob().submit();

	}

	/**
	 * Add all common word inside an array list in order to purge the colocation
	 * (this method is used in the Mapper step)
	 * @return
	 */
	public static ArrayList<String> getCommonWord(){
		if(commonWord == null){
			commonWord = new ArrayList<String>();
			ResultScanner scanner = HbaseWrapper.scanFullTable(HbaseTable.commonWord);

			for(Result rr : scanner){
				String sCommonWord = Bytes.toString(rr.getRow());
				commonWord.add(sCommonWord);
			}
		}
		return commonWord;
	}
}
