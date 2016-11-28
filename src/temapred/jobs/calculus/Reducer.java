package temapred.jobs.calculus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;
import temapred.utils.math.Statistic;
import temapred.utils.math.Time;

public class Reducer extends TableReducer<Text, Text, Text> {	
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
		if(Starter.totalCountForProportion == null || Starter.totalCountMap == null){
			Starter.totalCountForProportion = HbaseWrapper.scanFullTable(HbaseTable.totalCountPerHour);
			Starter.totalCountMap = new Hashtable<Long, Integer>();
			/* calcul du count en fonction du timestamp */
			for(Result r: Starter.totalCountForProportion){
				for(KeyValue kv : r.raw()){
					String sDate = Bytes.toString(kv.getRow());
					String sHour = Bytes.toString(kv.getFamily());
					String sCount = Bytes.toString(kv.getValue());

					Starter.totalCountMap.put(
							Time.getTimeOfDayAndHour(sDate + " " + sHour), 
							Integer.parseInt(sCount)
					);
				}
			}
		}

		ArrayList<InternObject> sortInternal = new ArrayList<InternObject>();
		for (Text val : values) {
			String [] decode = val.toString().split("\\|");
			if(decode.length == 2){
				long time = Time.getTimeOfDayAndHour(decode[0]);
				String value = decode[1];
				sortInternal.add(new InternObject(time, Integer.parseInt(value)));
			}
		}

		/* gestion du temps */
		Collections.sort(sortInternal);

		Put toPut = null;
		long diffInHour = 0;
		int n = 0, count = 0, unitProp = 0;
		double value = 0, mean_inc = 0, std = 0, z = 0, realProp = 0;
		/* iteration pour chaque date et chaque mot,, calcul des statistiques */
		for(int i = 0; i < sortInternal.size(); i++){
			Date date =  sortInternal.get(i).getDate();
			
			String word = key.toString();
			//String sDateOnly = formatterDate.format(date);
			//String sHourOnly = formatterHour.format(date);

			if(i == 0){
				diffInHour = 
					Math.abs((sortInternal.get(i).getTime() - 
							Starter.minDate.getTime()) / (1000 * 60 * 60));
			}else{
				diffInHour = Math.abs((sortInternal.get(i).getTime() - 
						sortInternal.get(i - 1).getTime()) / (1000 * 60 * 60));
			}

			
			if(diffInHour > 1){
				Date current = null;
				for(int j = 1; j < diffInHour; j++){
					if(i == 0 && j == 1){
						current = new Date(Starter.minDate.getTime() + (1000* 60 * 60));
					}else if(j == 1){
						current = new Date(sortInternal.get(i - 1).getTime() + (1000* 60 * 60));	
					}else{
						current = new Date(current.getTime() + (1000* 60 * 60));	
					}
					int thisZeroValue = 0;
					//sDateOnly = formatterDate.format(current);
					//sHourOnly = formatterHour.format(current);
					std = Statistic.standard_deviation_inc(std, mean_inc, n, thisZeroValue);
					mean_inc = Statistic.mean_inc(mean_inc, n, thisZeroValue);		
					count = 0;
					if(Starter.totalCountMap.get(current.getTime()) != null && Starter.totalCountMap.get(current.getTime()) != 0)
						value = (double)count/(double) Starter.totalCountMap.get(current.getTime());
					else
						value = 0;
					if(std !=  0)								
						z = (value - mean_inc) / (std);
					n++;
					// Insert dans Hbase toutes les stats
					realProp = ((double)unitProp / (double)n) * 100;
					
					toPut = toInsert(word, mean_inc, std, z, value, n, current.getTime(), realProp);
					if(word.startsWith("laden"))
						System.out.println(String.format("[ADDED] d: %s, w: %s, mean: %s, std: %s, prop: %s, n: %s, count: %s, z: %s, prop: %s",
								current,word,mean_inc,std,value,n,count,z,realProp));

					context.write(key, toPut);
					
				}
			}else{
				unitProp++;
			}
			
			count = sortInternal.get(i).getValue();
			value = (double)count/(double) Starter.totalCountMap.get(date.getTime());
			if(i == 0){
				mean_inc = Statistic.mean_inc(0, n, value);		
				std = Statistic.standard_deviation_inc(0, 0, n, value);		
			}else{
				std = Statistic.standard_deviation_inc(std, mean_inc, n, value);
				mean_inc = Statistic.mean_inc(mean_inc, n, value);	
			}

			
			if(std !=  0)
				z = (value - mean_inc) / (std);
			n++;
			
			if(i == 0) 
				unitProp++;
			
			realProp = (unitProp / (double)n) * 100;
			/* Insert dans Hbase toutes les stats */
			toPut = toInsert(word, mean_inc, std, z, value, n, date.getTime(), realProp);		
			context.write(key, toPut);
			
			System.out.println(String.format("d: %s, w: %s, mean: %s, std: %s, prop: %s, n: %s, count: %s, z: %s, prop: %s",
						date,word,mean_inc,std,value,n,count,z, realProp));
			
		}
	}

	private Put toInsert(String w, double mean, double std, double z, 
			double x, double n, long timestamp, double realProp){
		/* key: word, calculus: [mean | std | zscore | x | n | realProp], timestamp: date (nbDays), value */
		Put put = new Put(Bytes.toBytes(w));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("mean"), (timestamp), Bytes.toBytes(mean));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("std"), (timestamp), Bytes.toBytes(std));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("z"), (timestamp), Bytes.toBytes(z));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("x"), (timestamp), Bytes.toBytes(x));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("n"), (timestamp), Bytes.toBytes(n));
		put.add(Bytes.toBytes("calculus"),Bytes.toBytes("realprop"), (timestamp), Bytes.toBytes(realProp));
		return put;
	}
}

class InternObject implements Comparable<InternObject>{
	private long time;
	private int value;
	public InternObject(long time, int value) {
		this.time = time;
		this.value = value;
	}

	public long getTime(){
		return time;
	}

	public int getValue(){
		return value;
	}

	public int compareTo(InternObject o) {
		if(o.getTime() > this.getTime()) return -1;
		if(o.getTime() < this.getTime()) return 1;
		return 0;
	}

	public Date getDate(){
		return new Date(time);
	}

	public String toString(){
		return "[Intern] Time: " + time + " Value: " + value;
	}
}
