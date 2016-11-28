package temapred.utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Log {
	
	private static Logger logger = null;
	private static FileHandler fileHandler = null;
	
	private static Logger initLogger(){
		if(logger == null)
			logger = Logger.getLogger("LogMapReduceApplication");
		return logger;
	}
	
	private static FileHandler initFileHandler() throws SecurityException, IOException{
		if(fileHandler == null){
			fileHandler = new FileHandler("mapreduceApp.log", true);
		}
		return fileHandler;
	}
	
	public static void writeCritical(String msg){        
		
		try {
			initLogger().addHandler(initFileHandler());
		} catch (Exception e) {
			System.err.println("Enable to create the logger" + e);
		}

        if (logger.isLoggable(Level.SEVERE)) {
            logger.info(msg);
        }
	}
	
	public static void writeTrace(String msg){

		try {
			initLogger().addHandler(initFileHandler());
		} catch (Exception e) {
			System.err.println("Enable to create the logger" + e);
		}

        if (logger.isLoggable(Level.INFO)) {
            logger.info(msg);
        }
	}
}
