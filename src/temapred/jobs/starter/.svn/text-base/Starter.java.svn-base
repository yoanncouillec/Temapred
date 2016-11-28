package temapred.jobs.starter;

import temapred.jobs.common.BatchJob;
import temapred.jobs.common.ParallelJob;
import temapred.utils.Importer;
import temapred.utils.Log;

public class Starter {
	public static void main(String[]args){
		boolean redoImportSample = false; // temp will remove later and add a arg parameter
		String path = "";
		
		if(args.length != 0){
			path = args[0];
			redoImportSample = true;
		}
		
		if(redoImportSample){
			Importer importer = new Importer();
			try{
				/** Be careful the file format is: [Id,"0000-00-00 00:00:00","body"] **/
				if(path == "") path = "csv/opinions200000.csv";
				System.out.println("[Starting importer]");
				importer.importFile(path);
			}catch(Exception e){
				Log.writeCritical("[Importer] Error during the importer - Should exit now");
				System.exit(-1);
			}
		}

		
		System.out.println("[Starting wordcountperhour]");
		BatchJob wordcountperhour = new temapred.jobs.wordcountperhour.Starter();
		
		System.out.println("[Starting calculus]");
		BatchJob calculus = new temapred.jobs.calculus.Starter();
		System.out.println("[Starting detectTrends]");
		//BatchJob detectTrends = new temapred.jobs.detecttrends.Starter();
		
		/** General count: 1) per word 2) per date **/
		System.out.println("[Starting wordcountperDate]");
		//BatchJob wordCountPerDate = new temapred.jobs.wordcountperhour.Starter();
		System.out.println("[Starting totalCountPerDate]");
		//BatchJob totalcountperdate = new temapred.jobs.totalcountperdate.Starter();

		/** stop word extraction based on the standard deviation**/
		System.out.println("[Starting calculusPerCommonWord]");
		//BatchJob calculuspercommonword = new temapred.jobs.calculuspercommonword.Starter();
		System.out.println("[Starting stopWordExtraction]");
		//BatchJob stopwordextraction = new temapred.jobs.stopwordextraction.Starter();

		/** colocation extraction without common word**/
		System.out.println("[Starting colocationPerDate]");
		ParallelJob colocationPerDate = new temapred.jobs.colocationperdate.Starter();
		
		try{
			/*wordCountPerDate.runMe();
			totalcountperdate.runMe();
			calculuspercommonword.runMe();
			stopwordextraction.runMe();
			colocationPerDate.runMe();
			colocationPerDate.runMe();
			wordcountperhour.runMe();
			calculus.runMe();
			detectTrends.runMe();*/
		}catch(Exception e){
			e.printStackTrace();
			Log.writeCritical("[MapReduce MainStarter] " + e);
		}
	}
}
