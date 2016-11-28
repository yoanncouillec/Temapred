package temapred.utils;

import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import temapred.utils.hbase.HbaseTable;
import temapred.utils.hbase.HbaseWrapper;

public class Importer {

	/***
	 * Import a random sample from Opinion table
	 * HBase deamon should be run 
	 * 
	 * @throws Exception
	 */
	
	public void importFile(String fileFullPath) throws Exception {
		Log.writeTrace("Opening data csv file");
		
		HbaseWrapper.checkTableAndOverrideIfExists(HbaseTable.sample, "id");
		
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable htable = new HTable(hbaseConfig, HbaseTable.sample);
		htable.setAutoFlush(false);

		BufferedReader streamFile = Files.getBufferedReader(fileFullPath);

		if(streamFile == null){
			System.err.println("File doesn't exist");
			System.err.println("Exiting...");
			System.exit(1);
		}

		String currentLine;
		int cpt = 0;
		while((currentLine = streamFile.readLine()) != null)
		{
			if(cpt++ == 0) continue; //on saute l'entete du fichier

			String [] splitData = currentLine.split(",");

			if(splitData.length < 3) continue; // on vire les opinions vide

			try{
				Integer.parseInt(splitData[0]); // je verifie juste que c bien un ID
			}catch(NumberFormatException exception){
				continue;
			}
			
			String date = splitData[1].replace("\"", "");
			String time = "";
			if(splitData[1].indexOf(" ") > 0){
				time = date.substring(date.indexOf(" "),date.length());
				date = date.substring(0,date.indexOf(" ")); // la date pas l'heure dans ce job
			}
			
			long timestamp = 0;
				
			if(time != ""){
				try{
					String [] timeArray = time.replace(" ","").split(":");
					
					if(timeArray.length != 3) // oups c'est pas 00:00:00 
						throw new NumberFormatException();
					
					long hour = Long.parseLong(timeArray[0]);
					long minutes = Long.parseLong(timeArray[1]); 
					long second = Long.parseLong(timeArray[2]);
					// on converti tout en seconde
					timestamp = (hour * 60 * 60) + (minutes * 60) + second;
				}catch(NumberFormatException exception){
					timestamp = 0;
				}	
			}
			
			if(date.length() != 10) continue; // exactement 10 YYYY-MM-DD
			
			Put put = new Put(Bytes.toBytes(date));

			//Prend l'integralité a partir du body
			StringBuffer opinions = new StringBuffer();
			for(int i = 2; i < splitData.length; i++){
				opinions.append(splitData[i]);
			}

			if(cpt % 5000 == 0){
				Log.writeTrace("[Importer working on: " + 
						fileFullPath + " -> " + 
						cpt + " rows inside HBase");
			}
			put.add(
					Bytes.toBytes("id"), 
					Bytes.toBytes(splitData[0]),
					timestamp, 
					Bytes.toBytes(opinions.toString())
					);
			htable.put(put);
		}

		htable.flushCommits();
		htable.close();
		Log.writeTrace("Job Done");
	}
}

