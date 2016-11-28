package temapred.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class Files {
	public static BufferedReader getBufferedReader(String path){
		try {
			return new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			return null;
		}
	}
}
