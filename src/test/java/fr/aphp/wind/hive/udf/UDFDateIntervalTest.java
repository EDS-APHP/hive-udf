package fr.aphp.wind.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
/*
public class UDFDateIntervalTest {

	private UDFDateInterval udf;

	@Before
	public void before() {
		udf = new UDFDateInterval();
	}

	@Test
	public void testGetDateDiffYearOk() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		String dateEndString = "2019-05-15 00:00:00";
		Date d8End = new Date();

		String dateStartString = "2010-01-01 00:00:00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());
		
		
		Double actual = udf.evaluate(d8StartTimeStamp, d8EndTimeStamp,"DATE_DIFF_YEAR");
		Double expected = new Double(9.3);
		Assert.assertEquals(expected, actual);

	}

	@Test
	public void testGetDateDiffYearNull() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String dateEndString = "2019-05-15 00:00:00";
		Date d8End = new Date();

		String dateStartString = "2010-01-01 00:00:00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		
		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());
		
		Double actual = udf.evaluate(d8StartTimeStamp, null, "DATE_DIFF_YEAR");
		Double expected = new Double(9.3);
		Assert.assertNull("the return value is null", actual);
	}

	@Test
	public void testGetDateDiffYearDateError() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		String dateEndString = "2019-17-45 00:00:00";
		Date d8End = new Date();

		String dateStartString = "2010-01-01 00:00:00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		DateTime d8EndToda = new DateTime(d8End);
		
		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());

		// System.out.println("the month:" + d8EndToda.getMonthOfYear());
		// System.out.println("the day:" + d8EndToda.getDayOfMonth());

		Double actual = udf.evaluate(d8StartTimeStamp,d8EndTimeStamp, "DATE_DIFF_YEAR");
		Double expected = new Double(10.4);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testGetDateDiffDays() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		String dateEndString = "2019-05-15 18:30:00";
		Date d8End = new Date();

		String dateStartString = "2019-05-10 17:30:00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		// Handle the ParseException e ParseException here
		
		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());

		Double actual = udf.evaluate(d8StartTimeStamp, d8EndTimeStamp, "DATE_DIFF_DAYS");
		Double expected = new Double(5.04);
		Assert.assertEquals(expected, actual);
	}

	
	@Test
	public void testGetDateDiffYearOkTimestamp() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		String dateEndString = "2019-06-07 15:43:18.56";
		Date d8End = new Date();

		String dateStartString = "2018-06-20 17:25:52.00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());
		
		
		Double actual = udf.evaluate(d8StartTimeStamp, d8EndTimeStamp,"DATE_DIFF_YEAR");
		Double expected = new Double(0.9);
		Assert.assertEquals(expected, actual);

	}
	
	@Test
	public void testGetDateDiffDaysOkTimestamp() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		String dateEndString = "2019-06-07 15:43:18.56";
		Date d8End = new Date();

		String dateStartString = "2018-06-20 17:25:52.00";
		Date d8Start = new Date();

		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		Timestamp d8StartTimeStamp = new Timestamp();
		d8StartTimeStamp.setTimeInMillis(d8Start.getTime());
		
		Timestamp d8EndTimeStamp = new Timestamp();
		d8EndTimeStamp.setTimeInMillis(d8End.getTime());
		
		
		Double actual = udf.evaluate(d8StartTimeStamp, d8EndTimeStamp,"DATE_DIFF_DAYS");
		Double expected = new Double(351.93);
		Assert.assertEquals(expected, actual);

	}
}

 */
