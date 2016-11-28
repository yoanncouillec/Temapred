package temapred.jobs.calculus;

import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;
import temapred.utils.math.Time;

public class Starter extends BatchJob {
	public static Date minDate = null;
	public static Date maxDate = null;
	public static ResultScanner totalCountForProportion;
	public static Map<Long, Integer> totalCountMap;
	
	public void runMe() throws Exception {
		ResultScanner scanner = HbaseWrapper.scanFullTable(HbaseTable.wordcountperhour);
		
		// Ici nous voulons prendre la borne inf et sup de la periode d'analyse, l'avantage est
		// que c'est la min date et max date de la table wordCountPerHour
		
		Result first = scanner.next();
		long min = Time.getTimeOfDay(Bytes.toString(first.getRow()));
		long max = min;
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			long current = Time.getTimeOfDay(Bytes.toString(rr.getRow()));
			
			if(current < min)
				min = current;
			
			if(current > max)
				max = current;
		}
		
		minDate = new Date(min);
		maxDate = new Date(max);

		/*[mean | std | z | x | n | realprop]*/
		HbaseWrapper.checkCalculusTableAndOverrideIfExists(HbaseTable.calculus, "calculus");
		
		HbaseWrapper.launchMapReduceJob(
				"test", 
				HbaseTable.wordcountperhour,
				HbaseTable.calculus,
				new Scan(),
				Starter.class, 
				Mapper.class, 
				Reducer.class, 
				Text.class, 
				Text.class);
	}
}
