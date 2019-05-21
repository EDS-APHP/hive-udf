package fr.aphp.wind.hive.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;

public class UDFDateFunctionsTest {

	private UDFDateFunctions udf;

	@Before
	public void before() {
		udf = new UDFDateFunctions();
	}


	@Test
	public void testGetDateDiffYearOk() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String dateEndString = "2019-05-15";
		Date d8End = new Date();
				
		String dateStartString = "2010-01-01";
		Date d8Start = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		Double actual = udf.evaluate(d8Start, d8End, "Y");
		Double expected = new Double(9.3);
		Assert.assertEquals(expected, actual);

	}

	@Test
	public void testGetDateDiffYearNull() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String dateEndString = "2019-05-15";
		Date d8End = new Date();
				
		String dateStartString = "2010-01-01";
		Date d8Start = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		Double actual = udf.evaluate(d8Start, null, "Y");
		Double expected = new Double(9.3);
		Assert.assertNull("the return value is null",  actual);
	}


	@Test
	public void testGetDateDiffYearDateError() {

		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String dateEndString = "2019-17-45";
		Date d8End = new Date();
				
		String dateStartString = "2010-01-01";
		Date d8Start = new Date();
		
		try {
			d8Start = sdf.parse(dateStartString);
			d8End = sdf.parse(dateEndString);
		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException e ParseException here

		DateTime d8EndToda = new DateTime(d8End);
				
		//System.out.println("the month:" + d8EndToda.getMonthOfYear());
		//System.out.println("the day:" + d8EndToda.getDayOfMonth());
		
		Double actual = udf.evaluate(d8Start, d8End, "Y");
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
		} // Handle the ParseException e ParseException here

		Double actual = udf.evaluate(d8Start, d8End, "D");
		Double expected = new Double(5.04);
		Assert.assertEquals(expected, actual);
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

		Integer actual = udf.evaluate(d8Start, d8End);
		Integer expected = new Integer(14);
		Assert.assertEquals(expected, actual);
	}


	@Test
	public void testGetDateFromDateTime() {

		DateFormat sdfParameter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		DateFormat sdfExpected = new SimpleDateFormat("dd/MM/yyyy");

		String dateStringParameter = "15/05/2019 12:41:30";
		String dateStringExpected = "15/05/2019";

		Date d8Parameter = new Date();
		Date d8Expected = new Date();

		try {
			d8Parameter = sdfParameter.parse(dateStringParameter);
			d8Expected = sdfExpected.parse(dateStringExpected);

		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Date actual = udf.evaluate(d8Parameter);
		Assert.assertEquals(d8Expected, actual);

	}

	@Test
	public void testGePartOfDateTime() {

		DateFormat sdfParameter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		String dateStringParameter = "15/05/2019 12:41:30";

		Date d8Parameter = new Date();

		try {
			d8Parameter = sdfParameter.parse(dateStringParameter);

		} catch (ParseException e) {
			e.printStackTrace();
		} // Handle the ParseException here

		Integer actual = udf.evaluate(d8Parameter, DateEnum.HOUR_PART);
		Integer expected = new Integer(12);
		Assert.assertEquals(expected, actual);
	}

}
