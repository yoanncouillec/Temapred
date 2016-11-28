package temapred.utils.math.test;

import org.junit.Assert;
import org.junit.Test;

import temapred.utils.math.Time;


public class TestTime {
	@Test
	public void getInterval(){
		long secondes = 3599;
		Assert.assertEquals(0, Time.getHourInterval(secondes));
		secondes = 3600*24;
		Assert.assertEquals(24,Time.getHourInterval(secondes));
	}
	
	@Test
	public void testParsing(){
		String sDate = "2006-08-01 01";
		long time = Time.getTimeOfDayAndHour(sDate);
		Assert.assertTrue(time != 0);
		System.out.println(time);
	}
}
