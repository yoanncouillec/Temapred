package temapred.utils.math;

/**
 * Compute mean by increment, variance and standard deviation by increment.
 */
public class Statistic {
	public static double mean(double[] values)
	{
		double sum = 0;
		for (int i = 0; i < values.length; i++){
			sum += values[i];
		}
		return sum/(double) values.length;
	}

	/**
	 * Compute mean of n values with knowing mean of n - 1 values plus the value
	 * to be added.
	 * @param Mean Mean of n - 1 values.
	 * @param Value Value to add.
	 * @param n Cardinal of source set of values.
	 * @return Incremental mean.
	 */
	public static double mean_inc (double mean, int n, double value)
	{
		return mean + (value - mean) / (double) (n + 1);
	}

	/**
	 * Compute mean of n values with knowing mean of n + 1 values plus the value
	 * to be removed.
	 * @param mean Mean of n + 1 value.
	 * @param value Value to remove.
	 * @param n Cardinal of source set of values.
	 * @return Decremental mean.
	 */
	public static double mean_dec(double mean, int n, double value)
	{
		return ((double) n * mean - value) / ((double) (n - 1));
	}

	/**
	 * Compute mean in a window.
	 * @param xi Value to be removed.
	 * @param xj Value to be added.
	 * @param mean Mean of n left values.
	 * @param n Cardinal of target/source set of values.
	 * @return Mean for values from xi+1 to xj.
	 */
	public static double mean_win(double mean, int n, double xi, double xj)
	{
		return mean_dec (mean_inc(mean, n, xj), n + 1, xi);
	}

	/**
	 * S-term
	 * @param sterm Last s-term.
	 * @param mean Last mean.
	 * @param n Number of value at the start.
	 * @param value Value to be added.
	 * @return New s-term.
	 */
	private static double sterm_inc (double sterm, double mean, int n, 
			double value)
	{
		double ret = sterm + (value - mean) * (value - mean_inc(mean, n, value));
		return ret;
	}

	/**
	 * S-term
	 * @param sterm Last s-term.
	 * @param mean Last mean.
	 * @param n Number of value at the start.
	 * @param value Value to be removed.
	 * @return New s-term.
	 */
	private static double sterm_dec (double sterm, double mean, int n, 
			double value)
	{
		return sterm - (value - mean) * (value - mean_dec(mean, n, value));
	}

	/**
	 * Variance of an array of values..
	 * @param values Array of values.
	 * @return Variance of the values.
	 */
	public static double variance(double[] values)
	{
		double mean = mean(values);
		double sum = 0;
		for (int i = 0; i < values.length; i++){
			double tmp = values[i] - mean;
			sum += tmp * tmp;
		}
		return sum / (double) (values.length);
	}

	public static double standard_deviation(double[] values)
	{
		return Math.sqrt(variance(values));
	}

	/**
	 * Standard deviation by increment.
	 * @param standard_deviation Last standard deviation.
	 * @param mean Last mean.
	 * @param n Number of value at the start.
	 * @param value Value to be added.
	 * @return New standard deviation of the set of values.
	 */
	public static double standard_deviation_inc (double standard_deviation,
			double mean, int n, double value)
	{
		double sterm = n * standard_deviation * standard_deviation;
		return Math.sqrt (sterm_inc (sterm, mean, n, value) / (double) (n + 1));
	}

	/**
	 * Standard deviation by decrement.
	 * @param standard_deviation Last standard deviation.
	 * @param mean Last mean.
	 * @param n Number of value at the start.
	 * @param value Value to be removed.
	 * @return New standard deviation of the set of values.
	 */
	public static double standard_deviation_dec (double standard_deviation,
			double mean, int n, double value)
	{
		double sterm = n * standard_deviation * standard_deviation;
		return Math.sqrt (sterm_dec (sterm, mean, n, value) / (double) (n - 1));
	}

	/**
	 * Standard deviation in a window.
	 * @param standard_deviation Last standard deviation.
	 * @param mean Last mean.
	 * @param n Number of values in the started set.
	 * @param xi Value to be removed.
	 * @param xj Value to be added.
	 * @return New standard deviation.
	 */
	public static double standard_deviation_win (double standard_deviation,
			double mean, int n, double xi, double xj)
	{
		return standard_deviation_dec(
				standard_deviation_inc(standard_deviation, mean, n, xj), 
				mean_inc(mean, n, xj), 
				n+1, 
				xi);
	}
}























