package temapred.jobs.wordcountperhour;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;
import temapred.utils.math.Time;

public class Starter extends BatchJob {
	public static int nbWordInThisHour = 0;
	public static HTablePool pool = new HTablePool(HBaseConfiguration.create(), 10);
	public void runMe() throws Exception {
		ResultScanner scanner = pool.getTable(HbaseTable.sample).getScanner(new Scan());

		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.wordcountperhour, 
				"0","1","2","3","4","5","6","7","8","9","10","11","12","13","14",
				"15","16","17","18","19","20","21","22","23");

		ArrayList<String> days = new ArrayList<String>();
		for (Result rr : scanner) {
			days.add(Bytes.toString(rr.getRow()));
		}
		days.remove(0);
		days.remove(0);
		
		scanner.close();
		
		long min = 0;
		long max = 3600;
		List<Put> toPut = new ArrayList<Put>();
		int stop = 1;
		int skip = 0;
		for(String day: days){
			System.out.println("Analyse de d: " + day);
			Scan scanPerHour = new Scan();
			scanPerHour.setStartRow(Bytes.toBytes(day));
			scanPerHour.setStopRow(Bytes.toBytes(day));
			scanPerHour.setCaching(100);			
			for(int j = 0; j < 24; j++){
				scanPerHour.setTimeRange(min, max);	
				
				HbaseWrapper.launchMapReduceJob(
						day + "_" + (Time.getHourInterval(min)), 
						HbaseTable.sample,
						HbaseTable.wordcountperhour,
						scanPerHour,
						Starter.class, 
						Mapper.class, 
						Reducer.class, 
						Text.class, 
						IntWritable.class);

				Put put = new Put(Bytes.toBytes(day));
				put.add(Bytes.toBytes(String.valueOf(Time.getHourInterval(min))),
						Bytes.toBytes(String.valueOf(Starter.nbWordInThisHour)),
						Bytes.toBytes(String.valueOf(Starter.nbWordInThisHour)));

				toPut.add(put);
				Starter.nbWordInThisHour = 0;
				min = max;
				max += 3600;
			}
			System.out.println("fin jour d: " + day);
			min = 0;
			max = 3600;
			if(stop++ > 1) break;
		}
		this.totalCountPerHour(toPut);
	}
	
	public void totalCountPerHour(List<Put> list) throws IOException{
		HTable thisTotalCount = new HTable(HBaseConfiguration.create(),HbaseTable.totalCountPerHour);
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.totalCountPerHour, 
				"0","1","2","3","4","5","6","7","8","9","10","11","12","13","14",
				"15","16","17","18","19","20","21","22","23");
		thisTotalCount.put(list);
		thisTotalCount.close();
	}
}
