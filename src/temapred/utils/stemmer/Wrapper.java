package temapred.utils.stemmer;

import temapred.utils.Log;
import temapred.utils.stemmer.lang.englishStemmer;

public class Wrapper {
	public static String stem(String word){
		try {
			SnowballStemmer stemmer = englishStemmer.class.newInstance();
			stemmer.setCurrent(word);
			stemmer.stem();
			return stemmer.getCurrent();
		} catch (Exception e){
			Log.writeTrace("[Stemmer] Unable to stem the word " + word);
			return word;
		}
	}
	
	public static void main(String[]args){
		// Un simple test
		String word = "fishing";
		System.out.println(stem(word));
	}
}
