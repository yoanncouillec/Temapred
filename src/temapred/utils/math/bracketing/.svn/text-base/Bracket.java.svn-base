package temapred.utils.math.bracketing;

public class Bracket {
	private int start;
	private int end;
	private double zScorePalier;
	
	public Bracket(int start, int end, double zScorePalier){
		this.start = start;
		this.end = end;
		this.zScorePalier = zScorePalier;
	}
	
	public boolean isInside(double data){
		return data >= start && data < end;
	}
	
	public boolean isTrend(double data){
		if(isInside(data)){
			return data < zScorePalier;
		}
		return false;
	}
}
