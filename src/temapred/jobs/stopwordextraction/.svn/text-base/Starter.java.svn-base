package temapred.jobs.stopwordextraction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import temapred.jobs.common.BatchJob;
import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;

public class Starter extends BatchJob{
	public void runMe() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		ResultScanner scanner = HbaseWrapper.scanFullTable(HbaseTable.calculusPerWord);
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.commonWord, "std");
		HTable tableCommonWord = new HTable(conf, HbaseTable.commonWord);
		
		for (Result rr : scanner) {
			byte[] stdValue = (
					rr.getValue(
							Bytes.toBytes("calculus"), 
							Bytes.toBytes("standardDeviation")
					)
			);
			byte[] meanValue = (
					rr.getValue(
							Bytes.toBytes("calculus"), 
							Bytes.toBytes("mean")
					)
			);
			
			double standardDeviation = (Bytes.toDouble(stdValue));
			double mean = (Bytes.toDouble(meanValue));
			String word = Bytes.toString(rr.getRow());
			
			if(standardDeviation < 0.3 && mean > 0.0014){
				System.out.println("Mean = " + (mean*100) + " | word = " + word);
				Put put = new Put(Bytes.toBytes(word));
				put.add(Bytes.toBytes("std"), Bytes.toBytes(standardDeviation), Bytes.toBytes(mean));
				tableCommonWord.put(put);
			}
		}
	}
}
