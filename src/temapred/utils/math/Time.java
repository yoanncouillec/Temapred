package temapred.utils.math;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import temapred.utils.Log;

public class Time {
	public static long getHourInterval(long timestamp){
		return (timestamp / 3600);
	}
	
	/***
	 * 
	 * @param formatDate should be: 2011-04-06 H 
	 * @return
	 */
	public static long getTimeOfDayAndHour(String formatDate){
		SimpleDateFormat thisDateFormat = new SimpleDateFormat("yyyy-MM-dd H");
		try {
			Date thisDate = thisDateFormat.parse(formatDate);
			return thisDate.getTime();
		} catch (ParseException e) {
			Log.writeCritical("[PARSING DATE] Error: date " + formatDate + " " + e.toString());
			return 0;
		}
	}
	
	/***
	 * 
	 * @param formatDate should be: 2011-04-06 
	 * @return
	 */
	public static long getTimeOfDay(String formatDate){
		SimpleDateFormat thisDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date thisDate = thisDateFormat.parse(formatDate);
			return thisDate.getTime();
		} catch (ParseException e) {
			Log.writeCritical("[PARSING DATE] Error: date " + formatDate + " " + e.toString());
			return 0;
		}
	}
}
