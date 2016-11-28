package temapred.utils.math.test;
import org.junit.Test;

import temapred.utils.math.Statistic;

import junit.framework.Assert;

public class TestStatistic {
	
	double delta = 0.0000001;
	
	@Test
	public void mean(){
		double[] values = {14, 16, 18};
		Assert.assertEquals(16.0, Statistic.mean(values), delta);
	}
	
	@Test
	public void mean_inc(){
		Assert.assertEquals(16.0, Statistic.mean_inc(14.5, 2, 19), delta);
	}
	
	@Test
	public void mean_dec(){
		Assert.assertEquals(14.5, Statistic.mean_dec(16.0, 3, 19), delta);
	}
	
	@Test
	public void mean_win(){
		Assert.assertEquals(17.0, Statistic.mean_win(16.0, 3, 14, 17), delta);
	}
	
	@Test
	public void variance(){
		double[] values = {14, 16, 18};
		Assert.assertEquals(8.0/3.0, Statistic.variance(values), delta);
	}
	
	@Test
	public void standard_deviation(){
		double[] values = {14, 16, 18};
		Assert.assertEquals(Math.sqrt(8.0/3.0), Statistic.standard_deviation(values), delta);
	}
	
	@Test
	public void standard_deviation_inc(){
		double[] values1 = {14, 16, 18};
		double value = 20;
		
		double[] values2 = {14, 16, 18, value};
		double mean1 = Statistic.mean(values1);
		double stdev1 = Statistic.standard_deviation(values1);
		double stdev2 = Statistic.standard_deviation(values2);
		Assert.assertEquals(stdev2, Statistic.standard_deviation_inc(stdev1, mean1, values1.length, value), delta);
	}
	
	@Test
	public void standard_deviation_dec(){
		double value = 14;
		double[] values1 = {value, 16, 18, 20};		
		double[] values2 = {16, 18, 20};
		
		double mean1 = Statistic.mean(values1);
		double stdev1 = Statistic.standard_deviation(values1);
		double stdev2 = Statistic.standard_deviation(values2);
		Assert.assertEquals(stdev2, Statistic.standard_deviation_dec(stdev1, mean1, values1.length, value), delta);
	}
	
	@Test
	public void standard_deviation_win(){
		double xi = 14;
		double xj = 20;
		double[] values1 = {xi, 16, 18};		
		double[] values2 = {16, 18, xj};
		
		double mean1 = Statistic.mean(values1);
		double stdev1 = Statistic.standard_deviation(values1);
		double stdev2 = Statistic.standard_deviation(values2);
		Assert.assertEquals(stdev2, Statistic.standard_deviation_win(stdev1, mean1, values1.length, xi, xj), delta);
	}
	
}
