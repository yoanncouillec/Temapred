package temapred.utils.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import temapred.utils.Log;

public class HbaseWrapper {
	private static Configuration configuration = null;

	/**
	 * Start a map-reduce job in a batch mode and wait for completion
	 * @param jobName Just the name of the job.
	 * @param tableName Name of the concerned table in "HBase".
	 * @param tableReducer Output table.
	 * @param tableScanner The scan instance with the columns, time etc.
	 * @param jobClassName Example class to find the Jar.
	 * @param mapperClassName Class of the mapper.
	 * @param reducerClassName Class of the reducer.
	 * @param outputKey Class of the output key.
	 * @param outputValue Class of the output value.
	 * @return True, if well-done, false otherwise.
	 */
	@SuppressWarnings("rawtypes")
	public static boolean launchMapReduceJob(
			String jobName, 
			String tableName, 
			String tableReducer,
			Scan tableScanner, 
			Class<?> jobClassName,
			Class<? extends TableMapper> mapperClassName, 
		    Class<? extends TableReducer> reducerClassName,
			Class<? extends WritableComparable> outputKey, 
			Class<? extends Writable> outputValue){
		
		Job thisJob;
		try {
			thisJob = initMapReduceJob(
					jobName, 
					jobClassName, 
					mapperClassName, 
					reducerClassName,
					outputKey, 
					outputValue);

			TableMapReduceUtil.initTableMapperJob(
					tableName, 
					tableScanner, 
					mapperClassName,
					outputKey, 
					outputValue, 
					thisJob);
			
			TableMapReduceUtil.initTableReducerJob(
					tableReducer, 
					reducerClassName, 
					thisJob);
		
			return thisJob.waitForCompletion(true);
        
		} catch (Exception e) {
			Log.writeCritical(e.toString());
		}
		
		return false;        
	}
	
	/**
	 * Start a map-reduce job and will NOT wait for completion
	 * @param jobName Just the name of the job.
	 * @param tableName Name of the concerned table in "HBase".
	 * @param tableReducer Output table.
	 * @param tableScanner The scan instance with the columns, time etc.
	 * @param jobClassName Example class to find the Jar.
	 * @param mapperClassName Class of the mapper.
	 * @param reducerClassName Class of the reducer.
	 * @param outputKey Class of the output key.
	 * @param outputValue Class of the output value.
	 * @return The job in order to know later if it is done or not.
	 */
	@SuppressWarnings({ "rawtypes" })
	public static Job launchParallelMapReduceJob(
			String jobName, 
			String tableName, 
			String tableReducer,
			Scan tableScanner, 
			Class<?> jobClassName,
			Class<? extends TableMapper> mapperClassName, 
		    Class<? extends TableReducer> reducerClassName,
			Class<? extends WritableComparable> outputKey, 
			Class<? extends Writable> outputValue){
		
		Job thisJob;
		try {
			thisJob = initMapReduceJob(
					jobName, 
					jobClassName, 
					mapperClassName, 
					reducerClassName,
					outputKey, 
					outputValue);

			TableMapReduceUtil.initTableMapperJob(
					tableName, 
					tableScanner, 
					mapperClassName,
					outputKey, 
					outputValue, 
					thisJob);
			
			TableMapReduceUtil.initTableReducerJob(
					tableReducer, 
					reducerClassName, 
					thisJob);
        
			return thisJob;
        
		} catch (Exception e) {
			Log.writeCritical(e.toString());
		}
		
		return null;      
	}

	
	
	/**
	 * Initialized a map-reduce job before starting it.
	 * @param jobName Name of this job.
	 * @param jobClassName Example class to find the Jar.
	 * @param mapperClassName Class of the mapper of the job.
	 * @param reducerClassName Class of the reducer of the job.
	 * @param outputKey Class of the output key.
	 * @param outputValue Class of the output value.
	 * @return this job well initialized :).
	 * @throws IOException
	 */
	@SuppressWarnings("rawtypes")
	public static Job initMapReduceJob(
			String jobName,
			Class<?> jobClassName,
			Class<? extends Mapper> mapperClassName, 
			Class<? extends Reducer> reducerClassName,
			Class<?> outputKey, 
			Class<?> outputValue) throws IOException{

		Job thisJob = new Job(HBaseConfiguration.create(), jobName);
		
		thisJob.setJarByClass(jobClassName);
		thisJob.setNumReduceTasks(10);
		thisJob.setMapperClass(mapperClassName);
		thisJob.setReducerClass(reducerClassName);

		thisJob.setMapOutputKeyClass(outputKey);
		thisJob.setMapOutputValueClass(outputValue);

		thisJob.setInputFormatClass(TableInputFormat.class);
		thisJob.setOutputFormatClass(TableOutputFormat.class);
	
		return thisJob;
	}

	public static Configuration getConfiguration(){
		if(configuration == null){
			configuration = new Configuration();
		}
		return configuration;
	}
	
	/**
	 * If table tablename exists, it will disable and drop the table before recreating it
	 * with column culumNames
	 * @param tableName
	 * @param columnNames
	 */
	public static void checkTableAndOverrideIfExists(String tableName, String ... columnNames){
		try {
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			
			if(admin.tableExists(tableName)){
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				Log.writeTrace("[MapReduce] Deleting table: " + tableName);
			}
		
			HTableDescriptor thisTable = new HTableDescriptor(tableName);
			for(String s: columnNames){
				HColumnDescriptor cf = new HColumnDescriptor(s);
				thisTable.addFamily(cf);
			}
			admin.createTable(thisTable);
			
			Log.writeTrace("[MapReduce] Creating table: " + tableName);
		} catch (IOException e) {
			Log.writeTrace("[MapReduce] Error while checking table " + tableName);
		}
	}

	/**
	 * Specific for (ONLY!) calculus table
	 * @param tableName
	 * @param columnNames
	 */
	public static void checkCalculusTableAndOverrideIfExists(String tableName, String ... columnNames){
		try {
			Configuration hbaseConfig = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(hbaseConfig);
			
			if(admin.tableExists(tableName)){
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				Log.writeTrace("[MapReduce] Deleting table: " + tableName);
			}
		
			HTableDescriptor thisTable = new HTableDescriptor(tableName);
			
			for(String s: columnNames){
				HColumnDescriptor cf = new HColumnDescriptor(
						Bytes.toBytes(s), 
						365*2, // 2 ans d'historique
						HColumnDescriptor.DEFAULT_COMPRESSION,
						HColumnDescriptor.DEFAULT_IN_MEMORY, 
						HColumnDescriptor.DEFAULT_BLOCKCACHE, 
						HColumnDescriptor.DEFAULT_TTL, 
						HColumnDescriptor.DEFAULT_BLOOMFILTER);
				thisTable.addFamily(cf);
			}
			admin.createTable(thisTable);
			
			Log.writeTrace("[MapReduce] Creating table: " + tableName);
		} catch (IOException e) {
			Log.writeTrace("[MapReduce] Error while checking table " + tableName);
		}
	}
	
	/**
	 * Get a scanner in order to scan a full table
	 * @param tableName
	 * @return the scanner
	 */
	public static ResultScanner scanFullTable(String tableName){
		Configuration conf = HBaseConfiguration.create();
		HTable table;
		try {
			table = new HTable(conf, tableName);
			return table.getScanner(new Scan());
		} catch (IOException e) {
			Log.writeCritical("[Scanner] Error while getting scanner for " + tableName);
		}
		return null;
	}
	
	/**
	 * Get a scanner in order to scan a table from startRow to endRow
	 * @param tableName
	 * @param startRow
	 * @param endRow
	 * @return a scanner
	 */
	public static ResultScanner scanTable(String tableName, byte[] startRow, byte[] endRow){
		Configuration conf = HBaseConfiguration.create();
		HTable table;
		try {
			table = new HTable(conf, tableName);
			return table.getScanner(new Scan(startRow,endRow));
		} catch (IOException e) {
			Log.writeCritical("[Scanner] Error while getting scanner for " + tableName);
		}
		return null;
	}
}
