package temapred.utils.hbase;

public class HbaseTable {
	
	/* key: date, Id: idValue, value: body, Timestamp: second */
	public static String sample = "sample";                                                                       
	
	/* key: word, calculus: [standardDeviation | mean], value: value */
	public static String calculusPerWord = "calculusPerWord";                                                         
	
	/* key: date, count: nb, value: count */
	public static String totalCountPerHour = "totalCountPerHour";                                                              

	/* key: date, count: nb, value: count */
	public static String totalCountPerDate = "totalCountPerDate";  
	
	/* key: date, word: wordValue, value: count */
	public static String wordCountPerDate = "wordCountPerDate";   

	/* key: date, colocation: value, value: count */
	public static String colocationPerDate = "colocationPerDate"; 
	
	/* key: word, empty: empty, value: empty */
	public static String commonWord = "commonWord"; 
	
	/* key: date, valHour:valWord, value: count */
	public static String wordcountperhour = "wordcountperhour";
	
	/* key: word, calculus: [mean | std | zscore | x | value | n], timestamp: date (nbDays), value */
	public static String calculus = "calculus";
	
	
}
