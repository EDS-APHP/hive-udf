package fr.aphp.wind.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class UDFDateIntervalWithoutTimeTest {
	
	private UDFDateIntervalWithoutTime udf;

	@Before
	public void before() {
		udf = new UDFDateIntervalWithoutTime();
	}

	@Test
	public void testGetDateDiffAsDaysWithoutTime() {
		
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String dateEndString = "2019-05-15";
		Date d8End = new Date();
				
		String dateStartString = "2019-05-01";
		Date d8Start = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Integer actual = udf.evaluate(d8Start, d8End, DateEnum.DATE_DIFF_DAYS);
		Integer expected = new Integer(14);
		Assert.assertEquals(expected, actual);
	}
	@Test
	public void testGetDateDiffAsYearsWithoutTime() {
		
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String dateEndString = "2014-05-15";
		Date d8End = new Date();
				
		String dateStartString = "2009-05-01";
		Date d8Start = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Integer actual = udf.evaluate(d8Start, d8End, DateEnum.DATE_DIFF_YEAR);
		Integer expected = new Integer(5);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetDateDiffAsMonthsWithoutTime() {
		
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		
				
		String dateStartString = "2019-01-01";
		Date d8Start = new Date();
		
		String dateEndString = "2019-05-15";
		Date d8End = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Integer actual = udf.evaluate(d8Start, d8End, DateEnum.DATE_DIFF_MONTH);
		Integer expected = new Integer(4);
		Assert.assertEquals(expected, actual);
	}
}
