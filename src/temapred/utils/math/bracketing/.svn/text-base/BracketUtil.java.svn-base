package temapred.utils.math.bracketing;

import java.util.ArrayList;

public class BracketUtil implements IBracketing {
	public static ArrayList<Bracket> getAllBrackets(){
		ArrayList<Bracket> toRet = new ArrayList<Bracket>();
		toRet.add(new Bracket(0, 5, zScoreB1));
		toRet.add(new Bracket(5, 10, zScoreB2));
		toRet.add(new Bracket(10, 20, zScoreB3));
		toRet.add(new Bracket(20, 30, zScoreB4));
		toRet.add(new Bracket(30, 40, zScoreB5));
		toRet.add(new Bracket(40, 50, zScoreB6));
		toRet.add(new Bracket(50, 60, zScoreB7));
		toRet.add(new Bracket(60, 70, zScoreB8));
		toRet.add(new Bracket(70, 80, zScoreB9));
		toRet.add(new Bracket(80, 90, zScoreB10));
		toRet.add(new Bracket(90, 100, zScoreB11));
		return toRet;
	}
	
	public static boolean isTrend(double data){
		for(Bracket b : getAllBrackets()){
			if(b.isInside(data)){
				if(b.isTrend(data)){
					return true;
				}
			}
		}
		return false;
	}
}
